# br-drive

The "file collection" domain for services built on
[`br-service-engine`](https://github.com/BotResources/br-service-engine):
drives, files with a virtual path, verified direct-to-storage uploads, per-page
renditions and extracted images, labels, and the processing pipeline that hands
a file to a runner and stores what comes back. It is a **library**, not a
service: a host service embeds it as one slice, in its own database (schema
`drive` beside the host's schema) and under its own object-storage prefix. There
is no shared drive process anywhere and nothing flows between two hosts' files.
The library is content-blind — it never parses a file — and the host decides
access: every gesture asks the host's gate first, the library holds no
permission model of its own.

Two crates share one workspace version: the `br-drive` library and
`br-drive-example`, the fictional `workspace` service that embeds it — the
example host is the library's executable spec. Not published on crates.io: the
git tag is the release.

## Install

The workspace's MSRV is Rust 1.94 — the floor of the pinned `contract-jobs`
0.5.0, ahead of the engine's own 1.89.

```toml
[dependencies]
br-drive = { git = "https://github.com/BotResources/br-drive", package = "br-drive", tag = "v0.1.0", version = "0.1.0" }
```

The `version` beside the `tag` is required: a tag-only git dependency carries a
`*` version requirement, which `cargo-deny`'s `wildcards = "deny"` rejects.
`br-drive` pins one exact engine tag per minor; a host pins the same engine tag.

| br-drive | `br-service-engine` |
|---|---|
| 0.1 (unreleased) | `v0.3.0` |

## What a host writes

The engine's "library slice" path, exactly as `example-lib-roster` does it:

1. Implement `br_drive::DriveHost` on the host's principal type (below).
2. Embed the slice at the host's prefix and principal in `compose_service!`:

   ```rust
   service_engine::compose_service! {
       principal = crate::kernel::AppPrincipal;
       prefix = workspace;
       slice workspace ["workspace"] { query = …, mutation = …, subscription = … }
       slice drive ["drive"] from br_drive::drive_slice { query = drive::DriveQuery, mutation = drive::DriveMutation, subscription = drive::DriveSubscription }
   }
   ```

3. Pass `br_drive::migrations()` in `BootPlan.libraries`; `migrate` applies the
   engine set, then the library set (schema `drive`, band
   `9_121_000_001..=9_121_999_999`), then the host's, and grants the app role
   every schema. Configure object storage (`EngineConfig::with_blob_storage`);
   the library registers its `drive_source` blob kind when storage is configured.
   Start the runner-type catalogue mirror next to the engine:
   `br_drive::watch_runner_types(engine.nats().clone(), pool.clone())` returns
   a `CatalogueWatch` the host stops at shutdown (milestone 4, see "Processing
   rules").
4. Create and delete drives from the host's own mutations, in the host's own
   transaction: `br_drive::create_drive(cx, id, created_by)` and
   `br_drive::delete_drive::<AppPrincipal>(cx, id)` (cascades to the files and
   releases their blobs). A drive has no name and no row of its own on the wire:
   its id is the host object's id, it is the unit of visibility and the root of
   the cascade, nothing else. `br_drive::set_protected` marks a file the users
   may neither rename, move nor delete, `br_drive::set_metadata` writes the
   host's free JSON on a file (both refuse an unchanged value with
   `NOTHING_TO_CHANGE`), and `br_drive::drive_of` answers which drive a file
   belongs to.

Every root field the library contributes is prefixed by the host; the value
types (`DriveFile`, `DriveDelta`, …) keep their names in every embed, so a
downstream project pins one `br-drive` version across all its services and
rolls them together. The host never writes to the `drive` schema directly.

### The `DriveHost` seam

```rust
impl DriveHost for AppPrincipal {
    const SERVICE: &'static str = "workspace";          // the host service name
    const RUNNER_SCOPE: &'static str = "workspace:runner";
    const VISIBILITY_DEPS: Deps = Deps::from_bits(1 << OWNERSHIP_DEP);

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate { … }
    fn visible_drives(&self) -> Vec<Uuid> { … }
    fn display_name(&self) -> Option<String> { … }      // default None; feeds job.create's triggered_by
    fn upload_window(&self) -> Duration { … }           // default 15 min
    fn folder_moved(ops, drive, old_prefix, new_prefix) -> BoxFuture<…> { … }   // default no-op
    fn folder_deleted(ops, drive, prefix) -> BoxFuture<…> { … }                  // default no-op
}
```

`br_drive::register` (called by the slice's generated `register`) refuses to
boot a host that configured no object storage: the upload **is** the library,
so a storage-less embed fails loud instead of answering an internal error on
every `RequestUpload`.

- `drive_gate` receives the **request itself**, never only an action name:
  `CreateFile { drive, path, name, media_type, size }`, `ReadFile { file }`,
  `UpdateFile { file, target_drive }` (a cross-drive move names both drives),
  `DeleteFile { file }`, `MoveFolder { drive, old_prefix, new_prefix }`,
  `DeleteFolder { drive, prefix }`, `Process { file }`, `EditPage { file }`,
  `RegeneratePage { file, number }`, `ManageRulesets`, `ReadRulesets`,
  `ManageLabels`, `SetFileLabels { file }`. The host answers `Gate::allowed()`
  or `Gate::blocked(<its own reason code>)` — to refuse a media type it cannot
  render, or to restrict curation to the uploader (`file.created_by`). A file
  the host marked `protected` is refused with `FILE_PROTECTED` before the gate
  is asked. The same decision feeds the mutation guard and the `affordances`
  on `DriveFile` (`delete`, `rename`, `move`, `download`, `editPage`,
  `process`) and on `DrivePage` (`editPage`, `regeneratePage`), so the front
  renders and never decides.
- `visible_drives` is the cohort membership of the reactive views (dimension
  `drive`, `Cohort::uuid("drive", drive_id)`): a principal sees the files of the
  drives it lists. When that answer changes, the host stages
  `cx.impact_principal_facts(principal, deps)` with a dependency inside
  `VISIBILITY_DEPS`, and every open `DriveChanged` session repopulates — a lost
  drive arrives as `Remove` deltas, a gained one as `Upsert`s.
- `SOURCE_MAX_BYTES` (default 1 GiB) and `SOURCE_ORPHAN_AFTER` (default 24 h,
  must cover the engine's `upload_ttl`) are the `BlobPolicy` of the source kind.
  `upload_window` (default 15 min) should not be shorter than `upload_ttl`: a
  POST that lands after the deadline already released the row leaves an object
  on an orphaned blob, which the engine reaper deletes after `orphan_after`.
- `BULK_RESET_THRESHOLD` (default 256): `MoveFolder`, `DeleteFolder` and
  `delete_drive` run on the engine's **bulk** pipeline — one set-based
  statement for the rows, one blob release per file — and stage one caused
  impact per file up to the threshold, beyond it a projector reset (every open
  `DriveChanged` session receives a fresh `DriveReset`). So a folder or a drive
  of any size can be moved or deleted, whatever the engine's
  `impacts_per_commit`. The host registers the mutation that calls
  `delete_drive` with `register_bulk` (and answers it with `ack_bulk`).
- The two hooks run **inside** the `MoveFolder` / `DeleteFolder` transaction,
  after the bulk change, on the same `Ops`; a host that stores path prefixes
  elsewhere rewrites them there, and an `Err` rolls the whole gesture back (an
  `EngineError::PolicyRefused { code }` surfaces as that code).

## The GraphQL surface (milestone 2)

Root fields at the host prefix `<p>`; every mutation answers the engine's
`{ success }` ack or a coded refusal (`errors[].extensions.code`). Ids are
client-generated UUIDv7.

| Root | Shape |
|---|---|
| `<p>RequestUpload(fileId, driveId, path, name, mediaType, size: ByteCount, sha256): UploadTicket!` | gate `CreateFile` (asked before the drive is looked up, so an unknown drive id is not an existence oracle) → uniqueness (` (1)`, ` (2)` before the extension) under the drive's lock → verified presigned POST pinning the exact size and SHA-256 → the file row `PENDING`. `UploadTicket { fileId, url, fields }`: the front form-POSTs the bytes to `url` with `fields`. One transaction. `mediaType` must be a `type/subtype` token pair (`INVALID_MEDIA_TYPE`; the library never interprets it). |
| `<p>CommitUpload(fileId, rulesetId?): MutationAck!` | a live storage HEAD in the pending window: `UPLOAD_NOT_LANDED` unless the object is present and is the pinned bytes (a conforming store refuses anything else at upload); then the `upload` rule is picked (the given `rulesetId`, or the default matching the media type) and the chain starts (`PROCESSING`), or the file is `READY` when no rule matches. A second commit is `FILE_NOT_PENDING`. |
| `<p>Process(fileId, rulesetId?): MutationAck!` | re-process a `READY` or `FAILED` file with the `reprocess` rule (`NO_RULESET_MATCHES`, `RULESET_MISMATCH`, `FILE_PROCESSING` while a chain runs): the pages, the images (released) and the indexer triple are wiped at chain start. Affordance `process`. |
| `<p>RegeneratePage(fileId, number, comment?, rulesetId?): MutationAck!` | run the `regenerate_page` rule on one page of a `READY` file: `page` and `comment` are merged into the first step's options; `PAGE_NOT_FOUND`, `NO_RULESET_MATCHES`. Affordance `regeneratePage` on the page. |
| `<p>UpdateFile(fileId, name?, path?, driveId?): MutationAck!` | rename, move, or move to another drive of the same host; both drives are locked before the sibling check, so a concurrent collision answers `NAME_TAKEN`, never a database error; `NOTHING_TO_CHANGE` when nothing differs, `FILE_PROTECTED` on a protected file. |
| `<p>DeleteFile(fileId): MutationAck!` | cascade; the source blob is released in the same transaction. |
| `<p>MoveFolder(driveId, oldPrefix, newPrefix): MutationAck!` | bulk pipeline: one `UPDATE` on the prefix (the rows are locked first), then `folder_moved`; `FOLDER_NOT_FOUND`, `FOLDER_INTO_ITSELF`, `NAME_TAKEN` (refused as a whole), `FILE_PROTECTED` if any file under the prefix is protected, `INVALID_PATH` for the root or a rebased path over 1024 bytes. |
| `<p>DeleteFolder(driveId, prefix): MutationAck!` | bulk pipeline: one `DELETE` for every file under the prefix, one blob release per file, then `folder_deleted`. |
| `<p>File(fileId): DriveFile` | the file as the caller sees it, or `null`. |
| `<p>DriveFiles(driveId): [DriveFile!]!` | the drive's files as the caller sees them (the tree is a path prefix; empty folders do not exist). |
| `<p>Pages(fileId): [DrivePage!]!` | the file's rendition, page by page (milestone 3); empty when the caller cannot read the file. |
| `<p>Rulesets: [DriveRuleset!]!`, `<p>CreateRuleset(…)`, `<p>UpdateRuleset(…)`, `<p>DeleteRuleset(id)` | the host's processing rules (milestone 4, "Processing rules" below). |
| `<p>FileAccess(fileId, name: String): String` | a short-lived presigned GET on the source (attachment); `null` when the caller cannot see the file, a coded refusal when the `download` affordance is denied (`FILE_NOT_READY` before the commit, then the host's code), and `null` between the commit and the engine reaper's promotion (a verified blob is never downloadable in the pending window; the `SourceAvailable` cause says when to retry). `name` is reserved for the extracted images (milestone 3) and answers `null` today. |
| `<p>DriveChanged(driveId): DriveDelta!` | the file list: the engine snapshot on connect (`DriveReset` of `DriveFile`s), then `DriveUpsert` / `DriveRemove` on the contiguous revision. |
| `<p>FilePages(fileId): DriveDelta!` | one file's rendition (milestone 3): a `DriveReset` with every `DrivePage` of the file, then a `DriveUpsert` / `DriveRemove` per page; gated on `ReadFile` for the file's drive, so a caller who cannot read the file gets an empty window and a caller who loses the drive gets one `DriveRemove` per page. |

`DriveFile`: `id`, `driveId`, `path` (normalized, `""` = root), `name`,
`protected`, `mediaType`, `sizeBytes` (`ByteCount`, a 64-bit JSON number —
GraphQL `Int` is 32-bit), `sha256` (hex), `processingState`
(`PENDING | PROCESSING | READY | FAILED`), `processingError`, `metadata`
(JSON), `summary`, `pageCount`, `estimatedTokens`, `images[] { name,
mediaType, sizeBytes, page }`, `rulesetId`, `steps[] { runnerType, options }`
(the snapshot the last chain ran), `progress { stepIndex, stepCount,
runnerType, plan, currentIndex, currentLabel, at }` (non-null only while
`PROCESSING`), `createdBy`, `createdAt`, `updatedAt`, `affordances` (`delete`,
`rename`, `move`, `download`, `editPage`, `process` — `download` is denied
`FILE_NOT_READY` until the commit, `editPage` until the file is `READY` and
`FILE_PROCESSING` while a chain runs; `process` needs `READY` or `FAILED`).
The pages are
**not** on the file: `DrivePage { fileId, number, markdown, origin, updatedBy,
updatedAt, affordances { editPage } }` has its own projector, keyed by
`(fileId, number)`, its own query root and its own subscription, so a page
edit or a 300-page report never rewrites or re-emits the file row.

`DriveDelta` is the engine's delta union over `DriveView = DriveFile |
DrivePage`: `DriveReset { revision, views }`, `DriveUpsert { revision, view,
cause }`, `DriveRemove { revision, projector, key, cause }` (`projector` is
`drive_files` or `drive_pages`; a page key is `{ fileId, number }`), plus the
lane notices. `cause` is one of the library's file causes (`UploadRequested`,
`UploadCommitted`, `UploadAbandoned`, `SourceAvailable`, `Renamed`, `Moved {
from_drive }`, `FolderMoved`, `ProtectionChanged { protected }`,
`MetadataChanged`, `ImageRequested { name }`, `ImageAvailable { name }`,
`ImagesDropped { names }`, `ReportStored { job_id, done }`,
`ProcessingStarted { job_id, step }`, `ProgressChanged`,
`ProcessingFinished`, `ProcessingFailed { reason }`, `Deleted`,
`FolderDeleted`, `DriveDeleted`) or page causes (`Reported { job_id, origin
}`, `Edited`) on a delta the engine attributes to an impact; a key that
**enters or leaves** a live session's window (a created or deleted file, a
page the runner reports for the first time, a drive gained or lost) is
delivered by the engine's window repopulation and carries no cause in engine
0.3.0 (past the engine's reset threshold the entering keys arrive as one
`DriveReset`).

Abandoned uploads: `RequestUpload` schedules an `upload-deadline` reaction at
`now + upload_window`; a file still `PENDING` then is deleted (its blob is
released and the engine reaper removes the object). A file whose object landed
but was never committed is deleted the same way.

Paths: `/`-separated segments, no leading or trailing slash (both are
normalized away), no empty, `.` or `..` segment, no segment with leading or
trailing whitespace, no control character or backslash, segment ≤ 255 bytes,
whole path ≤ 1024 bytes, `""` is the root. Names obey the segment rules and
carry no `/`. Comparison is byte-wise: case-sensitive, no Unicode
normalization. Anything else is `INVALID_PATH` / `INVALID_NAME`.

## The runner contract (milestone 3)

A runner is a `Passport::Service` whose `scopes` claim carries the host's
`DriveHost::RUNNER_SCOPE` (e.g. `workspace:runner`); a human, or a service
without that scope, is refused every runner root with `RUNNER_SCOPE_REQUIRED`.
A runner sees nothing through the drive views and subscriptions — the host
keeps service principals out of `visible_drives` — and reads and writes one
file at a time through three roots at the host prefix, every one of them
gated on the file's active job (`JOB_NOT_ACTIVE`), reaching the host over the
gateway URL it already knows. The library is content-blind: it stores what the
runner reports and never interprets a media type.

| Root | Shape |
|---|---|
| `<p>RunnerContext(fileId, jobId): RunnerContext!` | `{ fileId, mediaType, name, pageCount, summary, pages[] { number, markdown, origin, updatedAt }, images[], sourceUrl }` — a **fresh** presigned GET on the source (inline) at every call, plus the current rendition read directly by file id, so an indexer or a page-regeneration runner reads the pages, not the source. `FILE_NOT_FOUND` for an unknown file, `JOB_NOT_ACTIVE` for any job but the file's, `SOURCE_NOT_AVAILABLE` while the engine reaper has not promoted the source yet (retry). |
| `<p>RunnerRequestImageUpload(fileId, jobId, name, mediaType, size: ByteCount, sha256): UploadTicket!` | a verified presigned POST for a `drive_image` blob (the runner hashes first; `FILE_TOO_LARGE` past `DriveHost::IMAGE_MAX_BYTES`) and the `file_image` row, unique per file by name. An existing name is **replaced only when the new object lands**: until then the old image stays readable and a failed replacement upload changes nothing; when it lands, the row swaps and the old object is released in the same transaction. A re-request for a name whose upload is still in flight is refused with `IMAGE_UPLOAD_PENDING` (the first ticket stands, one blob per request — the same posture as the source's `KEY_REUSED`); past the host's `upload_window` a re-request replaces the abandoned blob. |
| `<p>RunnerReport(fileId, jobId, pages: [ReportedPageInput!], origin: PageOrigin, summary, pageCount, estimatedTokens, done): MutationAck!` | pages in one or several batches of at most `MAX_REPORT_PAGES` (512, `BATCH_TOO_LARGE`), **upserted by number** (a replayed batch changes nothing; a number twice in one batch is `INVALID_PAGE`); each page reaches `<p>FilePages` on its own key (`Reported { job_id, origin }`) and the file row is touched only by the indexer's triple. `origin` `RUNNER` (default) or `REGENERATED` — on a regenerated page the images of that page that its new markdown no longer references (matched on the whole name, `![…](p001-img01.png)`, never as a substring) are dropped and released (`ImagesDropped { names }` on the file); `summary`, `pageCount` and `estimatedTokens` are the indexer's triple and move together (`INDEXER_FIELDS_TOGETHER`, `INVALID_INDEXER_VALUE` when negative; `ReportStored { job_id, done }` on the file when the triple changes); an empty report is `NOTHING_TO_CHANGE` unless `done`; `done: true` calls the `report_done` seam. |

The job. Every runner root validates `jobId` against the File's own `job_id`
— the job of the step that is running (`JOB_NOT_ACTIVE` for any other job, for
a file that never landed, and for a file that is not `PROCESSING`). `done:
true` on the report stages `integration.cmd.jobs.job.finish.v2 { job_id }` in
the report's transaction — the only path to a completed job; Jobs' `completed`
fact then moves the file to the next step or to `READY`.

Job config (what `job.create` carries, so a runner can be written against
it): `host` (the host service name), `file_id`, `job_id`, `context_root`
(`<p>RunnerContext`), `image_upload_root` (`<p>RunnerRequestImageUpload`),
`report_root` (`<p>RunnerReport`), `step` (the index in the rule), `options`
(the ruleset step's options; `options.page` and `options.comment` on a page
regeneration). The config never carries a presigned URL — the runner mints one
through `RunnerContext` when it needs it.

Image naming: `p{page:03}-img{n:02}.{ext}` — page-scoped, unique per file,
`ext` 1–8 lowercase alphanumerics, `n` starting at `01` (e.g. `p003-img01.png`);
anything else is `INVALID_IMAGE_NAME`. The markdown references images by that
name, never by URL; the front resolves a name to a presigned GET with
`<p>FileAccess(fileId, name)` (inline disposition; `null` for a caller who
cannot read the file, for an unknown name, and until the object landed). The
image's page is derived from its name.

Everything else a runner has to say — start, plan, steps, logs, completion,
failure, presence, cancel — goes to the Jobs service over its NATS runner
transport, never to the host.

Pages and images on the read side: `<p>Pages(fileId)` and the
`<p>FilePages(fileId)` subscription carry the rendition; `DriveFile` keeps
`summary`, `pageCount`, `estimatedTokens` and `images[]` (names and facts, no
bytes). A user edits a page of a `READY` file with `<p>EditPage(fileId,
number, markdown)` (origin `EDITED`, `PAGE_NOT_FOUND` for a page the runner
never reported; affordance `editPage` on the page) — one `DriveUpsert` on the
page, nothing on the file. Deleting a file (per row, per folder or with its
drive) drops its pages and images and releases every image object with the
source in the same transaction; the File aggregate never loads its rendition,
so a rename, a protection change, a metadata write or a folder gesture costs
the same on a 3-page file and on a 3000-page one.

## Processing rules (milestone 4)

Rulesets are per host service, declared at runtime by its managers, never
known to the front or the library by name. The table is empty at boot: until a
manager declares a rule, an upload stores the file and nothing else. A rule
says "when *trigger* happens on a file whose media type matches *mediaTypes*,
run *steps* in order" — a list, never a graph.

| Root | Shape |
|---|---|
| `<p>Rulesets: [DriveRuleset!]!` | `{ id, name, trigger, mediaTypes, steps[] { runnerType, options }, isDefault, createdBy, createdAt, updatedAt }`; empty for a caller the host's `ReadRulesets` gate refuses. |
| `<p>CreateRuleset(id, name, trigger: Trigger!, mediaTypes: [String!]!, steps: [RulesetStepInput!]!, isDefault): RulesetSaved!` | gate `ManageRulesets`; `trigger` `UPLOAD \| REPROCESS \| REGENERATE_PAGE`; `mediaTypes` are `type/subtype`, `type/*` or `*` (lowercased, `INVALID_MEDIA_TYPE`); `name` unique per host, case-insensitive (`RULESET_NAME_TAKEN`); 1–32 steps of `{ runnerType, options? }` (`INVALID_RULESET`). One default per (trigger, media-type pattern): a second default whose patterns overlap an existing default's is `DEFAULT_ALREADY_SET` (`*` is its own bucket). Answers `{ id, unknownRunnerTypes }` — the steps whose runner type is not `ACTIVE` in the Jobs catalogue, a warning at save time. |
| `<p>UpdateRuleset(id, name?, mediaTypes?, steps?, isDefault?): RulesetSaved!` | same rules; `NOTHING_TO_CHANGE` when nothing differs; the trigger is immutable. |
| `<p>DeleteRuleset(id): MutationAck!` | `RULESET_NOT_FOUND`; files keep the `rulesetId` and the `steps` snapshot of a deleted rule. |

Matching: the given `rulesetId` must exist (`RULESET_NOT_FOUND`) and carry
the gesture's trigger and a pattern matching the file's media type
(`RULESET_MISMATCH`); without an id the default of that trigger wins by
precedence exact > `type/*` > `*`. `CommitUpload` runs the `upload` rule or
lands `READY` when none matches. `Process` resolves in this order: the given
rule, else the default `reprocess` rule, else the file's own `steps` snapshot
(a replay of what last ran — so a failed upload chain can be retried without a
second rule), else `NO_RULESET_MATCHES` and the file is left as it is.
`RegeneratePage` takes the given rule, else the default `regenerate_page`
rule, else `NO_RULESET_MATCHES` (a regeneration cannot be inferred from an
upload snapshot). A rule never applies retroactively, and a rule edited or
deleted while a chain runs never reaches that chain: the File keeps a `steps`
snapshot taken when the rule fired. Saving a rule on a host whose catalogue
watch has never scanned is refused with `CATALOGUE_NOT_WATCHED`.

The chain, one step at a time, in the library's own transactions:

1. the step's runner type must be `ACTIVE` in the mirrored catalogue, else
   `FAILED` with `processingError = runner_type_unavailable` before any job
   (Jobs would not refuse an unknown type — it would wait); on a host whose
   catalogue watch has never scanned the chain fails `catalogue_not_watched`
   and an error is logged;
2. a `job_id` is minted on the File (`PROCESSING`, `progress { stepIndex,
   stepCount, runnerType }`) and `integration.cmd.jobs.job.create.v1` is
   staged through the engine outbox: `producer`, `source_bc` and `config.host`
   are the host service, `source_entity_id` is the file (so Jobs enforces one
   live job per file), `parent_job_id` the previous step's job,
   `triggered_by` the principal of the gesture (`DriveHost::display_name`);
3. the eight `integration.evt.jobs.job.*.v1` facts are consumed on eight
   durables named `{service}-drive-job-…` (every durable the library binds,
   the `upload-deadline` and `image-landed` ones included, is namespaced by
   `DriveHost::SERVICE`, so N hosts on one cluster never share a consumer)
   and matched on the File's `job_id` — a fact about a job no file holds is
   acknowledged and ignored, so every fact can be redelivered:
   `plan_declared` fills `progress.plan`, `step_started` moves
   `progress.currentIndex / currentLabel / at` (forward by index, or by a
   newer start instant when a retry attempt restarts the plan),
   `completed` launches the next step or lands `READY`
   (`ProcessingFinished`) — only once the runner's final report landed
   (`done_at`); a `completed` that arrives first is kept on the row and the
   report advances the chain — `creation_rejected` / `failed` land `FAILED` with
   the reason code (`ProcessingFailed { reason }`), `cancelled` lands `FAILED`
   `cancelled` — unless the cancel was ours: `DeleteFile`, `DeleteFolder` and
   `delete_drive` stage `job.cancel.v2` for every file they remove while it
   is `PROCESSING`, and the later `cancelled` fact finds no file;
4. the runner's `done: true` report records `done_at` and stages
   `job.finish.v2` (or advances the chain directly when Jobs already said
   `completed`).

`ruleset_id` and `steps` stay on the File for replay; `job_id`, `step_*`,
`plan` and `progress_*` are null outside `PROCESSING`.

The catalogue mirror: `drive.known_runner_type` is fed by
`br_drive::watch_runner_types` — a boot scan of `PUBLISHED_LANGUAGE` under
`jobs.runner_type.` (recorded in `drive.catalogue_scan`) then a KV watch,
tolerant of an entry that does not decode (warned and skipped) and strict on
the wire: an entry whose `version` is not `contract_jobs::runner::WIRE_VERSION`
is logged as an error and treated as unknown, never as active; the watch is
restarted after a fault. The host starts it next to the engine and stops it at
shutdown — engine 0.3.0 gives a library no boot or shutdown hook, so the
library cannot own that lifecycle; a host that forgets it is told loudly:
`CATALOGUE_NOT_WATCHED` on every rule save and `catalogue_not_watched` on
every chain, with an error log. It is a hand-rolled watch and not the
engine's mirror kit because the kit requires a `/`-terminated consumed prefix
and the catalogue is published by a non-engine producer under a dot prefix
with a per-value `version`; the watch is replaced by the kit when the kit
accepts such a prefix.

## Reason codes

`DRIVE_NOT_FOUND`, `FILE_NOT_FOUND`, `FOLDER_NOT_FOUND`, `FILE_PROTECTED`,
`FILE_NOT_PENDING`, `FILE_NOT_READY`, `FILE_PROCESSING`, `FILE_TOO_LARGE`,
`UPLOAD_NOT_LANDED`, `INVALID_SHA256`, `INVALID_MEDIA_TYPE`, `INVALID_PATH`,
`INVALID_NAME`, `NAME_TAKEN`, `FOLDER_INTO_ITSELF`, `NOTHING_TO_CHANGE`,
`KEY_REUSED`, `RUNNER_SCOPE_REQUIRED`, `JOB_NOT_ACTIVE`, `SOURCE_NOT_AVAILABLE`,
`INVALID_IMAGE_NAME`, `IMAGE_UPLOAD_PENDING`, `INVALID_PAGE`,
`INVALID_PAGE_ORIGIN`, `PAGE_NOT_FOUND`, `INDEXER_FIELDS_TOGETHER`,
`INVALID_INDEXER_VALUE`, `BATCH_TOO_LARGE`, `RULESET_NOT_FOUND`,
`RULESET_NAME_TAKEN`, `INVALID_RULESET`, `DEFAULT_ALREADY_SET`,
`RULESET_MISMATCH`, `NO_RULESET_MATCHES`, `RUNNER_TYPE_UNAVAILABLE`,
`CATALOGUE_NOT_WATCHED` — plus the host's own codes through the gate and the
hooks. On a `FAILED` file, `processingError` carries the runner's
`reason_code` verbatim, or one of the library's: `runner_type_unavailable`,
`catalogue_not_watched`, `cancelled`.

## Follow-ups

- The catalogue watch's health is not on the engine's readiness: the readiness
  assembly is the engine's, and its mirror-handle registration is a
  test-support API in 0.3.0. `drive.catalogue_scan` says whether a scan ever
  ran; a readiness reason for it needs the engine hook.
- Engine 0.3.0's `Query::download` populates the projector with its default
  window and then asks membership by key, so the runner's source presign
  cannot be told which file it is about: the `drive_runner_sources` projector
  enumerates every file id for that membership check, once per
  `RunnerContext` call, after the direct, keyed checks (runner scope, file,
  active job) passed — since milestone 4 only the files with a live job are
  enumerated (`job_id IS NOT NULL`, indexed); a key-aware download in the
  engine would remove the population altogether.

## The example host

`crates/br-drive-example` is the reference host: a thin kernel (principal,
facts, faults, the `DriveHost` impl), one `workspace` slice (the host object a
drive hangs off, owner-only gate: `workspaceCreate` / `workspaceDelete` /
`workspaceTransfer` / `workspaceProtectFile`; the `workspace:manage` scope on
a human passport is its `ManageRulesets` gate, any human reads the rules), the
embedded `drive` slice, the catalogue watch started at boot,
`src/bin/service.rs` handing everything to the engine boot kit, and `tests/`
— the harness spawns real PostgreSQL roles, `nats-server` and `minio`, boots
the host in process and drives it over GraphQL and a real
`graphql-transport-ws` socket. Jobs is played by a stand-in that publishes the
real `contract-jobs` DTOs on the real subjects and reads the commands the host
stages for `jobs`; the fake runner exercises the three runner roots.

## Running the example's suite

The suite needs a PostgreSQL superuser URL in `E2E_PG_ADMIN_URL` (fallback
`DATABASE_URL`), `nats-server` on `PATH`, and `minio` on `PATH` for the blob
scenarios (MinIO ≥ RELEASE.2024-12-13, for checksum conditions in POST
policies). `crates/br-drive-example/docker-compose.yml` brings up the three for
a local run of the binary itself.

```bash
E2E_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  cargo test --workspace --all-targets -- --test-threads=3
```

`BLESS_SCHEMA_FRAGMENTS=1 cargo test -p br-drive-example --test schema_fragments`
regenerates the committed per-slice SDL under `crates/br-drive-example/src/slices/*/schema.graphql`.

## AI disclosure

The code and the documentation of this repository were generated by an AI
system (Anthropic Claude) under the direction and review of BotResources.
BotResources takes full responsibility for them. This disclosure is made in
line with the transparency obligations of the EU Artificial Intelligence Act
(Regulation (EU) 2024/1689).

License: Apache-2.0.
