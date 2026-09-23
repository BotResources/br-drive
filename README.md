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
  `ManageLabels`, `SetFileLabels { file }`. The host answers `Gate::allowed()`
  or `Gate::blocked(<its own reason code>)` — to refuse a media type it cannot
  render, or to restrict curation to the uploader (`file.created_by`). A file
  the host marked `protected` is refused with `FILE_PROTECTED` before the gate
  is asked. The same decision feeds the mutation guard and the `affordances`
  on `DriveFile` (`delete`, `rename`, `move`, `download`), so the front renders
  and never decides.
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
| `<p>CommitUpload(fileId): MutationAck!` | a live storage HEAD in the pending window: `UPLOAD_NOT_LANDED` unless the object is present and is the pinned bytes (a conforming store refuses anything else at upload); else `READY` (no ruleset yet). A second commit is `FILE_NOT_PENDING`. |
| `<p>UpdateFile(fileId, name?, path?, driveId?): MutationAck!` | rename, move, or move to another drive of the same host; both drives are locked before the sibling check, so a concurrent collision answers `NAME_TAKEN`, never a database error; `NOTHING_TO_CHANGE` when nothing differs, `FILE_PROTECTED` on a protected file. |
| `<p>DeleteFile(fileId): MutationAck!` | cascade; the source blob is released in the same transaction. |
| `<p>MoveFolder(driveId, oldPrefix, newPrefix): MutationAck!` | bulk pipeline: one `UPDATE` on the prefix (the rows are locked first), then `folder_moved`; `FOLDER_NOT_FOUND`, `FOLDER_INTO_ITSELF`, `NAME_TAKEN` (refused as a whole), `FILE_PROTECTED` if any file under the prefix is protected, `INVALID_PATH` for the root or a rebased path over 1024 bytes. |
| `<p>DeleteFolder(driveId, prefix): MutationAck!` | bulk pipeline: one `DELETE` for every file under the prefix, one blob release per file, then `folder_deleted`. |
| `<p>File(fileId): DriveFile` | the file as the caller sees it, or `null`. |
| `<p>DriveFiles(driveId): [DriveFile!]!` | the drive's files as the caller sees them (the tree is a path prefix; empty folders do not exist). |
| `<p>Pages(fileId): [DrivePage!]!` | the file's rendition, page by page (milestone 3); empty when the caller cannot read the file. |
| `<p>FileAccess(fileId, name: String): String` | a short-lived presigned GET on the source (attachment); `null` when the caller cannot see the file, a coded refusal when the `download` affordance is denied (`FILE_NOT_READY` before the commit, then the host's code), and `null` between the commit and the engine reaper's promotion (a verified blob is never downloadable in the pending window; the `SourceAvailable` cause says when to retry). `name` is reserved for the extracted images (milestone 3) and answers `null` today. |
| `<p>DriveChanged(driveId): DriveDelta!` | the file list: the engine snapshot on connect (`DriveReset` of `DriveFile`s), then `DriveUpsert` / `DriveRemove` on the contiguous revision. |
| `<p>FilePages(fileId): DriveDelta!` | one file's rendition (milestone 3): a `DriveReset` with every `DrivePage` of the file, then a `DriveUpsert` / `DriveRemove` per page; gated on `ReadFile` for the file's drive, so a caller who cannot read the file gets an empty window and a caller who loses the drive gets one `DriveRemove` per page. |

`DriveFile`: `id`, `driveId`, `path` (normalized, `""` = root), `name`,
`protected`, `mediaType`, `sizeBytes` (`ByteCount`, a 64-bit JSON number —
GraphQL `Int` is 32-bit), `sha256` (hex), `processingState`
(`PENDING | PROCESSING | READY | FAILED`), `processingError`, `metadata`
(JSON), `summary`, `pageCount`, `estimatedTokens`, `images[] { name,
mediaType, sizeBytes, page }`, `createdBy`, `createdAt`, `updatedAt`,
`affordances` (`delete`, `rename`, `move`, `download`, `editPage` — the last
two are denied with `FILE_NOT_READY` until the file is `READY`). The pages are
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
`ImagesDropped { names }`, `ReportStored { job_id, done }`, `Deleted`,
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

The job seam. Until milestone 4 the library has no `job_id` column: each
runner root validates `jobId` against `DriveHost::active_job(&FileRow) ->
Option<Uuid>`, a host-provided seam. The example host's implementation — the
file's `metadata.job_id`, set through `workspaceAnnotateFile` — is the
example's stand-in for a Jobs integration, not a pattern a host should copy.
Milestone 4 replaces the seam with the File's own `job_id` and turns
`report_done` — the crate-private no-op called on `done: true` — into the
`integration.cmd.jobs.job.finish.v2` command.

Job config keys (what the host will put in `job.create`'s `config`, milestone
4, so a runner can already be written against them): `host` (the host service
name), `file_id`, `job_id`, `context_root` (`<p>RunnerContext`),
`image_upload_root` (`<p>RunnerRequestImageUpload`), `report_root`
(`<p>RunnerReport`), `step`, `options` (the ruleset step's options;
`options.page` on a page regeneration). The config never carries a presigned
URL — the runner mints one through `RunnerContext` when it needs it.

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

## Follow-ups

- Engine 0.3.0's `Query::download` populates the projector with its default
  window and then asks membership by key, so the runner's source presign
  cannot be told which file it is about: the `drive_runner_sources` projector
  enumerates every file id for that membership check, once per
  `RunnerContext` call, after the direct, keyed checks (runner scope, file,
  active job) passed. Milestone 4 restricts that population to the files with
  a live job; a key-aware download in the engine would remove it.

## The example host

`crates/br-drive-example` is the reference host: a thin kernel (principal,
facts, faults, the `DriveHost` impl), one `workspace` slice (the host object a
drive hangs off, owner-only gate: `workspaceCreate` / `workspaceDelete` /
`workspaceTransfer` / `workspaceProtectFile`), the embedded `drive` slice,
`src/bin/service.rs` handing everything to the engine boot kit, and `tests/`
— the harness spawns real PostgreSQL roles, `nats-server` and `minio`, boots
the host in process and drives it over GraphQL and a real
`graphql-transport-ws` socket.

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
