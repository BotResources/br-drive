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
| 0.1 | `v0.3.0` |
| 0.2 | `v0.3.4` |

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
4. Create and delete drives from the host's own mutations, in the host's own
   transaction: `br_drive::create_drive::<AppPrincipal>(cx, &workspace_row,
   created_by)` and `br_drive::delete_drive::<AppPrincipal>(cx, id)` (cascades
   to the files and releases their blobs). A drive has no name and no row of
   its own on the wire: it is created **from** its host object and takes that
   object's key, so its id is the host object's id by construction (there is
   no id parameter to get wrong); it is the unit of visibility and the root of
   the cascade, nothing else. A host whose `DriveOwner` is
   `br_drive::NoDriveOwner` has no host object to refresh and calls
   `br_drive::create_unowned_drive::<AppPrincipal>(cx, id, created_by)` with an
   id of its choosing instead — nothing in the library relies on it then.
   `br_drive::set_protected` marks a file the users
   may neither rename, move nor delete, `br_drive::set_metadata(ops, principal,
   file_id, metadata)` writes the host's free JSON on a file after asking the
   host's gate for that principal (`SetMetadata { file }`; both refuse an
   unchanged value with `NOTHING_TO_CHANGE`), and `br_drive::drive_of` answers which drive a file
   belongs to. `set_protected` is a host-internal function with no library
   gate: the host checks its own permission before calling it (the example
   host's `workspaceProtectFile` reserves it to the workspace owner).

Every root field the library contributes is prefixed by the host; the value
types (`DriveFile`, `DriveDelta`, …) keep their names in every embed, so a
downstream project pins one `br-drive` version across all its services and
rolls them together. The host never writes to the `drive` schema directly.


### Embedding checklist

1. **Compose**: `slice drive ["drive"] from br_drive::drive_slice { query = drive::DriveQuery, mutation = drive::DriveMutation, subscription = drive::DriveSubscription }` under the host's `prefix`; every root below appears at that prefix.
2. **Principals**: the engine's `register_reaction_principal` must resolve `Actor::Service` — every Jobs fact and both of the library's self-commands (`upload-deadline`, `image-landed`) arrive as a service actor, and a resolver that rejects services parks all ten reactions.
3. **`DriveHost`** on the principal: `SERVICE`, `RUNNER_SCOPE`, `IMPORT_SCOPE`, `type DriveOwner`, `VISIBILITY_DEPS`, the blob bounds (`SOURCE_MAX_BYTES`, `IMAGE_MAX_BYTES`, the two `*_ORPHAN_AFTER`), `BULK_RESET_THRESHOLD`, `drive_gate`, `visible_drives` (never a service principal), `display_name`, `upload_window`, `erase_mode`, the two folder hooks.
4. **Migrations**: `br_drive::migrations()` in `BootPlan.libraries` — schema `drive`, band `9_121_000_001..=9_121_999_999`, disjoint from the engine's reserved range and from the host's own.
5. **Object storage**: `EngineConfig::with_blob_storage` (the library refuses to register without it); two blob kinds, `drive_source` and `drive_image`; an S3-compatible store with POST-policy checksum conditions — MinIO ≥ `RELEASE.2024-12-13` — and a public endpoint the browser and the runners can reach for the presigned POST and GET.
6. **Jobs**: the outbox reaches `integration.cmd.jobs.>` and the eight `integration.evt.jobs.job.*.v1` subjects are on the `INTEGRATION_EVT` stream; the durables are named `{SERVICE}-drive-…`.
7. **Erase**: the engine's erase pipeline (`engine.eraser().erase(person)`) runs the library's `Erasable` in `DriveHost::erase_mode`; drives themselves are deleted by the host with `delete_drive`.
8. **Drives**: created and deleted from the host's own mutations (`create_drive` from the host object, or `create_unowned_drive` for a `NoDriveOwner` host, and `delete_drive` from a mutation registered with `register_bulk` and answered with `ack_bulk`); `set_protected`, `set_metadata`, `drive_of` for curation.
9. **Catalogue watch (optional, information only)**: `br_drive::watch_runner_types(engine.nats().clone(), pool.clone())` after boot, `CatalogueWatch::stop` at shutdown, fills the local copy of Jobs' runner-type catalogue that the save's `unknownRunnerTypes` warning and `<p>RunnerTypes` read. Only a host that starts it needs the `PUBLISHED_LANGUAGE` KV bucket on its broker. Nothing waits for it and no launch consults it: a host without it launches every step all the same, reports every step's type as unknown at save and lists no type.

### The `DriveHost` seam

```rust
impl DriveHost for AppPrincipal {
    const SERVICE: &'static str = "workspace";          // the host service name
    const RUNNER_SCOPE: &'static str = "workspace:runner";
    const VISIBILITY_DEPS: Deps = Deps::from_bits(1 << OWNERSHIP_DEP);
    const IMPORT_SCOPE: Option<&'static str> = Some("workspace:import"); // default None: no import
    type DriveOwner = Workspace;                        // or br_drive::NoDriveOwner

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate { … }
    fn visible_drives(&self) -> Vec<Uuid> { … }
    fn display_name(&self) -> Option<String> { … }      // default None; feeds job.create's triggered_by
    fn erase_mode() -> EraseMode { … }                  // default Anonymise; Delete removes the person's files
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
  `CreateFile { drive, path, name, media_type, size }`, `CommitUpload { file }`
  (the pending row, `created_by` included, so a shared drive can reserve the
  commit to the uploader), `ReadFile { file }`,
  `UpdateFile { file, target_drive }` (a cross-drive move names both drives),
  `DeleteFile { file }`, `MoveFolder { drive, old_prefix, new_prefix }`,
  `DeleteFolder { drive, prefix }`, `Process { file }`,
  `CancelProcessing { file }`, `EditPage { file }`,
  `RegeneratePage { file, number }`, `RetitleFile { file }`, `Import { file }`, `ImportCommit { file }`, `ManageRulesets`, `ReadRulesets`,
  `ManageLabels`, `ReadLabels`, `SetFileLabels { file }`,
  `SetMetadata { file }`. The host answers `Gate::allowed()`
  or `Gate::blocked(<its own reason code>)` — to refuse a media type it cannot
  render, or to restrict curation to the uploader (`file.created_by`). The
  host is asked **first**, the file's state second. A refusal about a file
  the principal cannot see (its drive is not in `visible_drives`) is
  answered `FILE_NOT_FOUND` — exactly what an unknown id answers, so neither
  the existence nor the state of an invisible file is ever disclosed. A
  principal who sees the file gets the host's own code, then the state
  refusals (`FILE_PROCESSING`, `FILE_NOT_READY`, `FILE_NOT_PENDING`,
  `FILE_PROTECTED`). A gesture addressed to a drive (`CreateFile`,
  `MoveFolder`, `DeleteFolder`) keeps the host's code, and a move toward an
  unknown drive answers the host's refusal before `DRIVE_NOT_FOUND`. The
  same decision feeds the mutation guard and the `affordances` on
  `DriveFile` (`delete`, `rename`, `move`, `retitle`, `download`, `editPage`,
  `process`, `cancelProcessing`, `setLabels`, `commit`, `setMetadata`) and on `DrivePage`
  (`editPage`, `regeneratePage`), so the front renders and never decides.
  The gate decides on the principal as the request found it: an engine
  principal's facts are loaded when the request arrives, so a host whose
  rule depends on a fact that may change concurrently (an ownership
  transfer, say) re-checks it under its own lock when that matters.
  `MoveFolder` / `DeleteFolder` ask the host about the folder first, then,
  for **every file under the prefix**, the very decision the per-file gesture
  asks — `UpdateFile { file, target_drive }` for a move, `DeleteFile { file }`
  for a delete, `FILE_PROTECTED` included — so a principal cannot move or
  delete through a folder a file it could not move or delete on its own. All
  or nothing: the first refusal in path order refuses the whole gesture with
  that code (the host's, or `FILE_PROTECTED`); a refusal about a file the
  principal cannot see (`FILE_NOT_FOUND`, above) anywhere in the folder
  answers `FOLDER_NOT_FOUND` for the whole gesture, what a prefix holding
  nothing answers — so an invisible file is neither named, nor placed, nor
  told apart from an empty folder. This holds as long as the host refuses a
  principal every file of a drive it does not see; a host that lets such a
  principal act on some files (an uploader rule, say) lets a folder gesture
  tell which case applied. The per-file decisions are in memory over the rows
  the gesture already loads: they add no statement.
- `type DriveOwner` — the host's own noun whose objects are keyed by the
  drive's id, marked `impl br_drive::DriveOwnerNoun for Its { type Object =
  ItsRow; }` (`ItsRow` the host aggregate, keyed by a UUID, that
  `create_drive` takes — so a drive's id is the key of the object the host
  declares, by construction rather than by convention; `DriveOwnerObject` is
  sealed); or
  `br_drive::NoDriveOwner` for a host with nothing to refresh. The noun is a
  type, so its name and its UUID key are checked by the compiler. Every file
  change the library stages — a file requested, committed, processed,
  failed, moved (both drives), retitled, labelled, deleted, a folder moved or
  deleted, an erase — also impacts that key, with no cause (the host's deltas
  keep speaking the host's own causes), so the host's views bound to its own
  noun recompute and republish; an object showing its drive's file counts
  stays live, and the engine's diff sends nothing when the view did not
  change. The library never calls the host: it stages an engine impact the
  host's projector already listens to. `br_drive::file_counts(conn,
  &drive_ids)` answers the files and READY files of several drives in one
  statement, served by an index. Cost: each such impact re-runs the window
  query of every live session holding a window on the host noun.
- `IMPORT_SCOPE` (default `None`): the scope a service account must hold to
  import a rendition (`<p>ImportPages`, `<p>ImportImage`) or commit an upload
  without processing (`<p>ImportCommit`). `None` means the
  host offers no import at all: the library refuses with
  `IMPORT_SCOPE_REQUIRED` before any gate, as it does for the runner scope.
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
client-generated UUIDv7. The complete SDL of the slice, as the example host
renders it, is committed at
`crates/br-drive-example/src/slices/drive/schema.graphql`.

| Root | Shape |
|---|---|
| `<p>RequestUpload(fileId, driveId, path, name, mediaType, size: ByteCount, sha256, title?): UploadTicket!` | `title` trimmed, 1–255 characters, one line, no control or bidirectional-override character (`INVALID_TITLE`); absent, it is the requested `name` without its extension (`report.pdf` → `report`; a name with no stem is kept whole; a collision renames the file, never its title); gate `CreateFile` (asked before the drive is looked up, so an unknown drive id is not an existence oracle) → uniqueness (` (1)`, ` (2)` before the extension) under the drive's lock → verified presigned POST pinning the exact size and SHA-256 → the file row `PENDING`. `UploadTicket { fileId, url, fields }`: the front form-POSTs the bytes to `url` with `fields`. One transaction. `mediaType` must be a `type/subtype` token pair (`INVALID_MEDIA_TYPE`; the library never interprets it). |
| `<p>CommitUpload(fileId, rulesetId?): MutationAck!` | gate `CommitUpload { file }` (the pending row, its uploader included), then `FILE_NOT_PENDING`; a live storage HEAD in the pending window: `UPLOAD_NOT_LANDED` unless the object is present and is the pinned bytes (a conforming store refuses anything else at upload); then the `upload` rule is picked (the given `rulesetId`, or the default matching the media type) and the chain starts (`PROCESSING`), or the file is `READY` when no rule matches. A second commit is `FILE_NOT_PENDING`. |
| `<p>Process(fileId, rulesetId?): MutationAck!` | re-process a `READY` or `FAILED` file with the `reprocess` rule (`NO_RULESET_MATCHES`, `RULESET_MISMATCH`, `FILE_PROCESSING` while a chain runs): the pages, the images (released) and the indexer triple are wiped at chain start. Affordance `process`. |
| `<p>CancelProcessing(fileId): MutationAck!` | gate `CancelProcessing { file }`, then `FILE_NOT_PROCESSING` unless the file is `PROCESSING`; logs `cancel_requested` on the running job and stages `job.cancel.v2` for it (`CancelRequested { job_id }`); the file stays `PROCESSING` until Jobs' `cancelled` lands it `FAILED` `cancelled`, open to a reprocess. Asking again sends the cancel again (Jobs may drop a cancel it consumes before the job's creation). Affordance `cancelProcessing`. |
| `<p>RegeneratePage(fileId, number, comment?, rulesetId?): MutationAck!` | run the `regenerate_page` rule on one page of a `READY` file: `page` and `comment` are merged into the first step's options; `PAGE_NOT_FOUND`, `NO_RULESET_MATCHES`. Affordance `regeneratePage` on the page. |
| `<p>UpdateFile(fileId, name?, path?, driveId?): MutationAck!` | rename, move, or move to another drive of the same host; both drives are locked before the sibling check, so a concurrent collision answers `NAME_TAKEN`, never a database error; `NOTHING_TO_CHANGE` when nothing differs, `FILE_PROTECTED` on a protected file. |
| `<p>RetitleFile(fileId, title): MutationAck!` | gate `RetitleFile { file }` (affordance `retitle`); changes the title and nothing else — never the name, the path, the drive or the state, and `UpdateFile` never touches the title; `INVALID_TITLE`, `NOTHING_TO_CHANGE` for the same title; the host decides whether a `protected` file may be retitled (the row is in the request). |
| `<p>DeleteFile(fileId): MutationAck!` | cascade; the source blob is released in the same transaction. |
| `<p>MoveFolder(driveId, oldPrefix, newPrefix): MutationAck!` | gate `MoveFolder`, then the `move` decision of every file under the prefix (all or nothing: the first refusal's code, `FILE_PROTECTED` included; `FOLDER_NOT_FOUND` for a file the caller cannot see); bulk pipeline: one `UPDATE` on the prefix (the rows are locked first), then `folder_moved`; `FOLDER_NOT_FOUND`, `FOLDER_INTO_ITSELF`, `NAME_TAKEN` (refused as a whole), `INVALID_PATH` for the root or a rebased path over 1024 bytes. |
| `<p>DeleteFolder(driveId, prefix): MutationAck!` | gate `DeleteFolder`, then the `delete` decision of every file under the prefix (same all-or-nothing rule); bulk pipeline: one `DELETE` for every file under the prefix, one blob release per file, then `folder_deleted`. |
| `<p>File(fileId): DriveFile` | the file as the caller sees it, or `null`. |
| `<p>DriveFiles(driveId): [DriveFile!]!` | the drive's files as the caller sees them (the tree is a path prefix; empty folders do not exist). |
| `<p>Pages(fileId): [DrivePage!]!` | the file's rendition, page by page (milestone 3); empty when the caller cannot read the file. |
| `<p>Rulesets: [DriveRuleset!]!`, `<p>CreateRuleset(…)`, `<p>UpdateRuleset(…)`, `<p>DeleteRuleset(id)` | the host's processing rules (milestone 4, "Processing rules" below). |
| `<p>Labels: [DriveLabel!]!` | the host's label catalogue: `DriveLabel { id, name, color, description, createdAt, updatedAt }` in id order (UUIDv7: creation order); gated by the host's `ReadLabels` — a refused principal (a runner, typically) gets an empty list and an empty live window, exactly as `ReadRulesets` does for the rules; both live windows repopulate when the principal's facts change, so gaining or losing the gate shows at once (the creator's id stays on the row, off the wire). |
| `<p>CreateLabel(id, name, color, description?)`, `<p>UpdateLabel(id, name?, color?, description?)`, `<p>DeleteLabel(id): MutationAck!` | gate `ManageLabels`; `name` trimmed, 1–100 characters, unique per host case-insensitive (`LABEL_NAME_TAKEN`); `color` `#rrggbb` lowercase hex (an uppercase input is lowercased; anything else `INVALID_LABEL`); `description` defaults to `""`, at most 1 KiB; the name check and the write are serialized, so two concurrent saves of one name answer exactly one `LABEL_NAME_TAKEN`; `NOTHING_TO_CHANGE` on an unchanged update. `DeleteLabel` runs on the bulk pipeline: it detaches the label from every file it was on — `LabelsChanged { detached }` per file up to `BULK_RESET_THRESHOLD`, a `DriveFiles` reset beyond — so a label on any number of files can go. |
| `<p>SetFileLabels(fileId, labelIds): MutationAck!` | gate `SetFileLabels { file }`; the target set, idempotent — the same set is `NOTHING_TO_CHANGE`, an unknown id `LABEL_NOT_FOUND`; `labelIds` on `DriveFile` follows (`LabelsChanged`), and a file keeps its labels across the host's drives. |
| `<p>FileAccess(fileId, name: String): String` | a short-lived presigned GET on the source (attachment); `null` when the caller cannot see the file, a coded refusal when the `download` affordance is denied (the host's code first, then `FILE_NOT_READY` before the commit), and `null` between the commit and the engine reaper's promotion (a verified blob is never downloadable in the pending window; the `SourceAvailable` cause says when to retry). With `name`, a presigned GET on that extracted image (inline; `null` for an unknown name and until the object landed). |
| `<p>DriveChanged(driveId): DriveDelta!` | the file list: the engine snapshot on connect (`DriveReset` of `DriveFile`s), then `DriveUpsert` / `DriveRemove` on the contiguous revision. |
| `<p>LabelsChanged: DriveDelta!` | the label catalogue live: a `DriveReset` of `DriveLabel`s, then an upsert (`Created`, `Updated`) or a remove per label. |
| `<p>RulesetsChanged: DriveDelta!` | the rule table live, for the manager's screen: an upsert per save with `Saved { unknown_runner_types }` as its cause (the same warning the save answers), a remove per delete. |
| `<p>FilePages(fileId): DriveDelta!` | one file's rendition (milestone 3): a `DriveReset` with every `DrivePage` of the file, then a `DriveUpsert` / `DriveRemove` per page; gated on `ReadFile` for the file's drive, so a caller who cannot read the file gets an empty window and a caller who loses the drive gets one `DriveRemove` per page. |

`DriveFile`: `id`, `driveId`, `path` (normalized, `""` = root), `name`,
`title` (the human-facing title, independent of the name),
`protected`, `mediaType`, `sizeBytes` (`ByteCount`, a 64-bit JSON number —
GraphQL `Int` is 32-bit), `sha256` (hex), `processingState`
(`PENDING | PROCESSING | READY | FAILED`), `processingError`, `metadata`
(JSON), `summary`, `pageCount`, `estimatedTokens`, `images[] { name,
mediaType, sizeBytes, page }`, `labelIds` (computed, by label name), `rulesetId`, `steps[] { runnerType, options }`
(the snapshot the last chain ran), `progress { stepIndex, stepCount,
runnerType, plan, currentIndex, currentLabel, at }` (non-null only while
`PROCESSING`), `createdBy`, `createdAt`, `updatedAt`, `affordances` (`delete`,
`rename`, `move`, `retitle`, `download`, `editPage`, `process`, `cancelProcessing`, `setLabels` — `download` is denied
`FILE_NOT_READY` until the commit, `editPage` until the file is `READY` and
`FILE_PROCESSING` while a chain runs; `process` needs `READY` or `FAILED`;
`cancelProcessing` needs `PROCESSING`, `FILE_NOT_PROCESSING` otherwise).
`processingState`, `processingError` and `progress` are computed, never
stored: see "Processing rules".
The pages are
**not** on the file: `DrivePage { fileId, number, markdown, origin, updatedBy,
updatedAt, affordances { editPage } }` has its own projector, keyed by
`(fileId, number)`, its own query root and its own subscription, so a page
edit or a 300-page report never rewrites or re-emits the file row.

`DriveDelta` is the engine's delta union over `DriveView = DriveFile |
DrivePage | DriveLabel | DriveRuleset`: `DriveReset { revision, views }`, `DriveUpsert { revision, view,
cause }`, `DriveRemove { revision, projector, key, cause }` (`projector` is
`drive_files` or `drive_pages`; a page key is `{ fileId, number }`), plus the
lane notices. `cause` is one of the library's file causes (`UploadRequested`,
`UploadCommitted`, `UploadAbandoned`, `SourceAvailable`, `Renamed`, `Retitled`, `Moved {
from_drive }`, `FolderMoved`, `ProtectionChanged { protected }`,
`MetadataChanged`, `ImageRequested { name }`, `ImageAvailable { name }`,
`ImagesDropped { names }`, `ReportStored { job_id, done }`, `RenditionImported`,
`ProcessingStarted { job_id, step }`, `ProgressChanged`,
`ProcessingFinished`, `ProcessingFailed { reason }`, `CancelRequested { job_id }`, `LabelsChanged {
detached }`, `Erased`, `Deleted`, `FolderDeleted`, `DriveDeleted`), page
causes (`Reported { job_id, origin
}`, `Edited`, `Imported { origin }`), label causes (`Created`, `Updated`, `Deleted`) or rule causes
(`Saved { unknown_runner_types }`, `Deleted`) on a delta the engine attributes
to an impact; a key that
**enters or leaves** a live session's window (a created or deleted file, a
page the runner reports for the first time, a drive gained or lost) is
delivered by the engine's window repopulation and carries no cause in engine
0.3.0 (past the engine's reset threshold the entering keys arrive as one
`DriveReset`).

Abandoned uploads: `RequestUpload` schedules an `upload-deadline` reaction at
`now + upload_window`; a file whose upload is still not confirmed then
(`committed_at` unset, so `PENDING`) is deleted (its blob is
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
| `<p>ImportPages(fileId, pages: [ImportedPageInput!]!, summary, pageCount, estimatedTokens): MutationAck!` | the host-privileged import of an existing rendition: a service account holding the host's `IMPORT_SCOPE` (`IMPORT_SCOPE_REQUIRED` otherwise, before anything else), then the gate `Import { file }`, then a `READY` file (`FILE_PROCESSING`, `FILE_NOT_READY` otherwise) — no job, no runner scope. Pages `{ number, markdown, origin? }` (`RUNNER` by default; `EDITED` keeps a page a person had corrected; `REGENERATED` is refused, `INVALID_PAGE_ORIGIN`; an imported page records the importer and the import time as its `updatedBy` / `updatedAt`) are upserted by number under the same rules as a report (≤ 512 per call, numbers ≥ 1 and distinct, the indexing pair together, the estimate optional); each reaches `<p>FilePages` (`Imported { origin }`), an indexing reaches the file (`RenditionImported`); the file stays `READY`; `NOTHING_TO_CHANGE` for an empty import. For a downstream project moving an existing corpus in without re-running its conversions. |
| `<p>ImportImage(fileId, name, mediaType, size: ByteCount, sha256): UploadTicket!` | same scope, gate and state; the runner's verified image path (page-scoped name, replacement on landing) without its job. |
| `<p>ImportCommit(fileId): MutationAck!` | commits a pending upload **without processing**: the same scope as an import (`IMPORT_SCOPE_REQUIRED` first, before any file is looked at), then the host's own gate `ImportCommit { file }` on the pending row (its uploader included — the example host reserves it to the migration account's own uploads), then `FILE_NOT_PENDING`, then the same live storage HEAD as `CommitUpload` (`UPLOAD_NOT_LANDED`); the file lands `READY` (`UploadCommitted`) and no rule runs, even when an `upload` rule matches — no chain, no `job.create`. For a host that declared its processing rules before migrating its corpus: upload, `ImportCommit`, then `ImportPages` / `ImportImage`. A normal `CommitUpload` is unchanged. |
| `<p>RunnerReport(fileId, jobId, pages: [ReportedPageInput!], origin: PageOrigin, summary, pageCount, estimatedTokens, done): MutationAck!` | pages in one or several batches of at most `MAX_REPORT_PAGES` (512, `BATCH_TOO_LARGE`), **upserted by number** (a replayed batch changes nothing; a number twice in one batch is `INVALID_PAGE`); each page reaches `<p>FilePages` on its own key (`Reported { job_id, origin }`) and the file row is touched only by the indexer's triple. `origin` `RUNNER` (default) or `REGENERATED` — on a regenerated page the images of that page that its new markdown no longer references (matched on the whole name, `![…](p001-img01.png)`, never as a substring) are dropped and released (`ImagesDropped { names }` on the file); `summary` and `pageCount` are the indexer's pair and move together, `estimatedTokens` is optional and only rides along with them (`INDEXER_FIELDS_TOGETHER` otherwise, `INVALID_INDEXER_VALUE` when negative; an indexing without an estimate clears a previous one; `ReportStored { job_id, done }` on the file when any of the three changes); an empty report is `NOTHING_TO_CHANGE` unless `done`; `done: true` stages `job.finish.v2` (see "Processing rules"). |

The job. Every runner root validates `jobId` against the file's running job —
the last job of its log while the file is `PROCESSING` (`JOB_NOT_ACTIVE` for
any other job, for a file that never landed, and for a file that is not
`PROCESSING`). `done:
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
`ext` 1–8 lowercase alphanumerics, `n` starting at `01` (e.g. `p003-img01.png`).
The widths are minimums: page 1000 is `p1000-…`, the hundredth image of a page
is `…-img100.…`, and a number is never padded wider than it needs
(`p0003-img01.png` is refused), so every image has exactly one name. Anything
else is `INVALID_IMAGE_NAME`. The markdown references images by that
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
| `<p>CreateRuleset(id, name, trigger: Trigger!, mediaTypes: [String!]!, steps: [RulesetStepInput!]!, isDefault): RulesetSaved!` | gate `ManageRulesets`; `trigger` `UPLOAD \| REPROCESS \| REGENERATE_PAGE`; `mediaTypes` are `type/subtype`, `type/*` or `*` (lowercased, `INVALID_MEDIA_TYPE`); `name` unique per host, case-insensitive (`RULESET_NAME_TAKEN`); 1–32 steps of `{ runnerType, options? }` (`INVALID_RULESET`). One default per (trigger, media-type pattern): a second default whose patterns overlap an existing default's is `DEFAULT_ALREADY_SET` (`*` is its own bucket). Answers `{ id, unknownRunnerTypes }`: the steps' runner types the local catalogue copy does not know as `ACTIVE` (absent or `DEPRECATED`), sorted, once each — a warning only, the rule is saved. Jobs alone judges a runner type, when the step's job is created (below). |
| `<p>UpdateRuleset(id, name?, mediaTypes?, steps?, isDefault?): RulesetSaved!` | same rules and the same warning; `NOTHING_TO_CHANGE` when nothing differs; the trigger is immutable. |
| `<p>RunnerTypes: [DriveRunnerType!]!` | the local copy of Jobs' runner-type catalogue, for a rule-editing screen: `{ runnerType, lifecycle: ACTIVE \| DEPRECATED, version, seenAt }` in name order (`version` is the entry's wire version, `seenAt` when the watch last wrote it). Gated by the host's `ReadRulesets`, like `<p>Rulesets`: whoever reads the rules may see the names they carry, and a refused principal gets an empty list. Empty on a host that does not run the catalogue watch. A plain read, not a live window: the watch writes outside the engine's pipelines and stages no impact. |
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
snapshot taken when the rule fired.

The chain, one step at a time, in the library's own transactions:

1. a step is a **job**: the library mints a UUIDv7 `job_id`, records it in the
   file's job log (`drive.file_job`: the job, its step index, its initiator,
   an empty log) and stages `integration.cmd.jobs.job.create.v1` through the
   engine outbox in the same transaction: `producer`, `source_bc` and
   `config.host` are the host service, `source_entity_id` is the file (so Jobs
   enforces one live job per file; `RequestUpload` refuses a file id that is
   not a UUIDv7 with `INVALID_FILE_ID`, since Jobs would refuse every job of
   it), `triggered_by` the principal of the gesture (`DriveHost::display_name`,
   trimmed, blank as anonymous, cut at 512 characters; an erased initiator is
   not named at all — what Jobs accepts), and **no `parent_job_id`**: the chain
   is a host-side sequence correlated by the file id, and every step is owned
   by the host (Jobs refuses a terminal parent, and a live one would make the
   step the parent runner's work);
2. the eight `integration.evt.jobs.job.*.v1` facts are consumed on eight
   durables named `{service}-drive-job-…` (every durable the library binds,
   the `upload-deadline` and `image-landed` ones included, is namespaced by
   `DriveHost::SERVICE`, so N hosts on one cluster never share a consumer).
   Each fact is **appended to the log of its own job** — `{kind, at,
   ...the fact's payload as received}`, in arrival order — after the job's
   file row is locked; a fact about a job no file holds is acknowledged and
   ignored, so every fact can be redelivered. Only the file's **last** job
   moves the file: `plan_declared` fills `progress.plan` and `step_started`
   moves `progress.currentIndex / currentLabel / at` (the latest start wins,
   whatever the arrival order; `ProgressChanged` only when the progress
   moved), `completed` launches the next step or lands `READY`
   (`ProcessingFinished`), `creation_rejected` / `failed` land `FAILED` with
   the reason code (`ProcessingFailed { reason }`), `cancelled` lands `FAILED`
   `cancelled`. A fact of an older job — or a fact of the last job after its
   first terminal fact (Jobs' facts ride separate durables, so a step may
   arrive after the completion) — is logged and changes nothing;
3. the runner's `done: true` report stages `job.finish.v2`; Jobs marks a job
   completed only on its owner's finish, so its `completed` fact is what
   advances the chain — the chain never moves on a report alone. A user's
   cancel that crossed the completion (Jobs refuses to cancel a finished job
   and says nothing) stops the chain there: the next step is recorded as a
   job row of its own, never sent to Jobs, whose only entry is the library's
   `cancelled`, and the file lands `FAILED` `cancelled`.

The state is **computed, never stored**. The `drive.file_status` view reads
each file's last job (the last recorded, by an identity sequence Postgres
assigns — never by the pods' clocks): no job and no `committed_at` → `PENDING`; no job and
`committed_at` set → `READY`; a last job whose first terminal entry is
`completed` → `READY` (the next step's job is appended in the transaction that
logs the completion, so a completed last job means the chain is over);
`failed`, `cancelled` or `creation_rejected` → `FAILED`, `processingError`
read from that entry (a runner's `failure_report.reason_code`, else Jobs'
`failure_cause`; a rejection's `reason_code`; `cancelled`); anything else, or
an empty log → `PROCESSING`. Every gate, affordance, view and the
`file_counts` helper read it; a gesture locks the file row before reading it,
so two gestures never both see the same last job, and a partial unique index
keeps at most one unsettled job per file whatever happens. `ruleset_id` and
`steps` stay on the File for replay; `progress` is `null` outside
`PROCESSING`.

A file stuck in `PROCESSING` has one way out, the user's
`<p>CancelProcessing` (above): the library keeps **no deadline** of its own.
Jobs refuses at creation a runner type it knows and no longer accepts jobs
for (`creation_rejected` `runner_type_retired`: the file fails at once), but
it accepts a runner type it does not know and never dispatches its job — as it
never fails a job no live runner picks up. Such a file waits in `PROCESSING`
until someone cancels it; Jobs' `cancelled` then lands it `FAILED`, and a
reprocess (a fixed rule, a started runner) starts over. The library's copy
of the runner-type catalogue (below) is information only: a rule may name any
runner type, and a step's job is created whatever the copy says. The
cancel relies on Jobs answering: a job Jobs holds as settled or does not know
at all (a host or Jobs database restored to an earlier point, a job deleted
in Jobs) never gets a `cancelled`, and its file stays `PROCESSING` — deleting
the file is then the way out (see Follow-ups).

A `creation_rejected` `duplicate_active_entity` means Jobs still holds a live
job on the file that the library does not count as live (a lost
`job.finish`, a restored host database, a cancel and a create that crossed on
Jobs' separate consumers). When the job Jobs names (`params.activeJobId`)
belongs to this file of this host (`sourceEntityId`, `sourceBc`), the library
stages `job.cancel.v2` for it; the rejection lands the file `FAILED`
`duplicate_active_entity`, and the user's reprocess gets through once Jobs
cancelled it (a cancel is idempotent). `DeleteFile`, `DeleteFolder` and
`delete_drive` stage `job.cancel.v2` for the running job of every file they
remove, and the later `cancelled` fact finds no file.

The catalogue copy: `drive.known_runner_type` (`runner_type`, `lifecycle`,
`version`, `seen_at`) is fed by the optional `br_drive::watch_runner_types` — a
scan of `PUBLISHED_LANGUAGE` under `jobs.runner_type.` then a KV watch; a type
Jobs retires leaves the bucket and the copy. It is tolerant of an entry that
does not decode or names another type than its key (warned and left out) and
strict on the wire: an entry whose `version` is not
`contract_jobs::runner::WIRE_VERSION` is logged as an error and left out. The
watch is restarted after a fault. The host starts it next to the engine and
stops it at shutdown — engine 0.3.0 gives a library no boot or shutdown hook.
It is read by the save's `unknownRunnerTypes` and by `<p>RunnerTypes`, never
by a gesture's decision: no readiness reason, no deferred launch, no refusal.
It is a hand-rolled watch and not the engine's mirror kit because the kit
requires a `/`-terminated consumed prefix and the catalogue is published by a
non-engine producer under a dot prefix with a per-value `version`.

The runner source presign (`<p>RunnerContext`'s `sourceUrl`) goes through the
`drive_runner_sources` view scoped to the one file the job names, and only
while that job is the file's running one — never a population of every
in-flight file.

## Erase

The library implements the engine's `Erasable` for its rows and registers it
at `br_drive::register`; the host drives it through the engine's erase
pipeline (`engine.eraser().erase(person)`), never through a GraphQL root, and
the pipeline records the erasure, purges what the manifest names and emits
the engine's `PersonErased` fact. `DriveHost::erase_mode` picks the mode:

- `Anonymise` (default): every `created_by` / `updated_by` the person left on
  drives, files, pages, labels, label links and rules, and the `triggered_by`
  of the jobs they started (on the job log), is rewritten to `br_drive::REDACTED_PERSON`
  (the nil UUID); nothing is deleted.
- `Delete`: every file the person created is deleted (its pages, images and
  label links cascade, its objects are purged through the manifest), then the
  rest is anonymised. A job still running on such a file is not cancelled —
  the erase pipeline's `Ops` has no outbound identity — the deleted file
  refuses the runner's next call, and the job is left to Jobs (its run
  backstops; a job no runner picked up waits for an administrator's cancel in
  Jobs — the file, and its cancel affordance, are gone). Drives
  are the host's to delete (`delete_drive`). The erase context cannot stage a
  projector reset, so live sessions are told file by file up to
  `BULK_RESET_THRESHOLD` and catch up on their next reset past it.

A second erase of the same person is absorbed by the engine (`fresh: false`)
and changes nothing.

## Reason codes

`DRIVE_NOT_FOUND`, `FILE_NOT_FOUND`, `FOLDER_NOT_FOUND`, `FILE_PROTECTED`,
`FILE_NOT_PENDING`, `FILE_NOT_READY`, `FILE_PROCESSING`, `FILE_TOO_LARGE`,
`UPLOAD_NOT_LANDED`, `INVALID_SHA256`, `INVALID_FILE_ID`, `INVALID_MEDIA_TYPE`, `INVALID_PATH`,
`INVALID_NAME`, `INVALID_TITLE`, `NAME_TAKEN`, `FOLDER_INTO_ITSELF`, `NOTHING_TO_CHANGE`,
`KEY_REUSED`, `RUNNER_SCOPE_REQUIRED`, `IMPORT_SCOPE_REQUIRED`, `JOB_NOT_ACTIVE`, `SOURCE_NOT_AVAILABLE`,
`INVALID_IMAGE_NAME`, `IMAGE_UPLOAD_PENDING`, `INVALID_PAGE`,
`INVALID_PAGE_ORIGIN`, `PAGE_NOT_FOUND`, `INDEXER_FIELDS_TOGETHER`,
`INVALID_INDEXER_VALUE`, `BATCH_TOO_LARGE`, `RULESET_NOT_FOUND`,
`RULESET_NAME_TAKEN`, `INVALID_RULESET`, `DEFAULT_ALREADY_SET`,
`RULESET_MISMATCH`, `NO_RULESET_MATCHES`, `FILE_NOT_PROCESSING`,
`LABEL_NOT_FOUND`, `LABEL_NAME_TAKEN`,
`INVALID_LABEL` — plus the host's own codes through the gate and the hooks. On a `FAILED` file, `processingError` carries the runner's
`reason_code` verbatim (Jobs' `creation_rejected` codes included, e.g.
`duplicate_active_entity`, `runner_type_retired`), Jobs' `failure_cause`
when the runner sent no report, or `cancelled`; a file that was `FAILED`
before migration `9121000007` keeps its stored error, and one found
`PROCESSING` without a job reads `interrupted`.

## Follow-ups

- The erase pipeline's `Ops` has no outbound identity and cannot stage a
  projector reset: a job running on a file deleted by an erase is left to
  Jobs' timeout, and past
  `BULK_RESET_THRESHOLD` deleted files, live `DriveChanged` sessions catch up
  on their next reset instead of receiving one `Remove` per file.
- `select_ruleset` reads the defaults of one trigger per gesture — fine at a
  host's scale (tens of rules), noted for the record.
- The catalogue watch stamps `seen_at` from the database clock: it runs
  outside the engine's `Ops` and has no engine clock. Its health is not on the
  engine's readiness (nothing depends on it), and `<p>RunnerTypes` is a plain
  read with no live window: the watch stages no impact.
- A job Jobs holds on a file the library cannot tell about is cancelled only
  on a `duplicate_active_entity` rejection: nothing polls Jobs. A manual
  retry of a failed job in Jobs is not followed either — the file settled on
  the job's first terminal fact (`FAILED`), later facts are logged only, and
  the runner of the retried run is refused `JOB_NOT_ACTIVE`; the user
  reprocesses instead.
- A file whose running job Jobs holds as settled or does not know (a restored
  database, a job deleted in Jobs) stays `PROCESSING`: the cancel is sent and
  never confirmed. A host-privileged gesture settling such a job locally is
  not written yet; deleting the file is the way out.
- A runner report the library refuses (an invalid batch, say) is a refused
  mutation: its transaction rolls back, so nothing is logged on the job and
  the runner decides whether to fail its run with Jobs.
- The Jobs double serializes the commands it reads, so the crossing of a
  `job.cancel` and a `job.create` on Jobs' separate durables is modelled by
  dropping cancels (`drop_cancels`), not reproduced as a race.
- Engine 0.3.0's `Query::download` populates the projector with its default
  window and then asks membership by key, so the runner's source presign
  cannot be told which file it is about through the window. The runner
  context resolver scopes the population to the job's own file (a task-local
  set around the download, `br_drive::scoped_to_job`), and the population
  re-checks that the job is still the file's active one; a key-aware download
  in the engine would remove the scoping.

## The example host

`crates/br-drive-example` is the reference host: a thin kernel (principal,
facts, faults, the `DriveHost` impl), one `workspace` slice (the host object a
drive hangs off, owner-only gate: `workspaceCreate` / `workspaceDelete` /
`workspaceTransfer` / `workspaceProtectFile`; the `workspace:manage` scope on
a human passport is its `ManageRulesets` gate, any human reads the rules), the
embedded `drive` slice (its `CancelProcessing` gate allows a workspace's
owner, like every per-file gesture), the catalogue watch started at boot
(`BootOptions::watch_catalogue`, on by default), a
`{"hold": true}` metadata rule refusing to move or delete a file (the per-file
rule the folder scenarios meet), a `workspace:sweep` scope allowed folder
gestures but no file, `src/bin/service.rs` handing everything to the engine boot kit, and `tests/`
— the harness spawns real PostgreSQL roles, `nats-server` and `minio`, boots
the host in process and drives it over GraphQL and a real
`graphql-transport-ws` socket. Jobs is played by a stand-in that publishes the
real `contract-jobs` DTOs on the real subjects and reads the commands the host
stages for `jobs`. It judges every `job.create` the way Jobs does, in Jobs' order — the inputs
`svc-jobs` refuses before its domain (non-v7 ids, a blank or over-long display
name, a source not named by its producer), a runner type it retired
(`runner_type_retired`, and withdrawn from the catalogue it publishes in
`PUBLISHED_LANGUAGE`; any other type is accepted, published or not), a reused
id, a second live job on one source entity (`duplicate_active_entity`,
naming the live job), a self-named, terminal, deleted or unknown parent —
answering `creation_rejected` on its own and `cancelled` for a live job it
cancels (or dropping every cancel, to model one Jobs consumed too early), so a
contract violation fails the suite instead of passing it; the fake runner exercises the three runner roots.

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
