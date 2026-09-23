# Changelog

All notable changes to `br-drive` are documented here. The whole workspace
ships **one version**: every crate inherits `version.workspace = true`, and a
single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## Unreleased

### Added

- Repository skeleton on the `br-service-engine` template, pinned on engine
  `v0.3.0`: the `br-drive` library crate with `br_drive::migrations()` (schema
  `drive`, band `9_121_000_001..=9_121_999_999`) and the roster-shaped
  `drive_slice!` macro (`prefix = …; principal = …`), the `DriveHost` bound the
  host principal implements, and the `br-drive-example` host — a fictional
  `workspace` service that embeds the slice through `compose_service!` and
  `BootPlan.libraries` — with its in-repo e2e harness (real PostgreSQL, NATS
  JetStream, `graphql-transport-ws`) and one smoke scenario.
- Drives and files by path. `drive.drive` and `drive.file` (`(drive_id, path,
  name)` unique, the source blob reference on the row, `processing_state`
  `PENDING | PROCESSING | READY | FAILED`). Library functions the host calls in
  its own transaction: `create_drive`, `delete_drive` (cascades to the files and
  releases their blobs), `set_protected`, `drive_of`. Value objects `DrivePath`
  (normalized: `/`-separated, no leading/trailing slash, no empty, `.` or `..`
  segment, segment ≤ 255 bytes, `""` = root) and `FileName` (`first_free` derives
  `name (1)`, `name (2)` before the extension on a sibling collision).
- The `DriveHost` seam: `SERVICE`, `RUNNER_SCOPE`, `VISIBILITY_DEPS`,
  `SOURCE_MAX_BYTES`, `SOURCE_ORPHAN_AFTER`; `drive_gate(&DriveRequest)` — the
  gate receives the request itself (the requested path, name, media type and
  size for `CreateFile`; the file row for `ReadFile` / `UpdateFile` /
  `DeleteFile` / `Process` / `EditPage` / `SetFileLabels`; both drives for a
  cross-drive move; the prefixes for `MoveFolder` / `DeleteFolder`);
  `visible_drives`; `upload_window`; and the two in-transaction hooks
  `folder_moved` and `folder_deleted` (default no-op; a refusal rolls the bulk
  gesture back).
- GraphQL roots at the host prefix: `<p>RequestUpload(fileId, driveId, path,
  name, mediaType, size: ByteCount, sha256)` → `UploadTicket { fileId, url,
  fields }` (a verified presigned POST pinning the exact size and SHA-256),
  `<p>CommitUpload(fileId)` (a live storage HEAD in the pending window; `READY`
  on commit),
  `<p>UpdateFile(fileId, name?, path?, driveId?)`, `<p>DeleteFile(fileId)`,
  `<p>MoveFolder(driveId, oldPrefix, newPrefix)`, `<p>DeleteFolder(driveId,
  prefix)`, `<p>File(fileId)`, `<p>DriveFiles(driveId)`, `<p>FileAccess(fileId,
  name?)` (a presigned GET, attachment disposition) and the
  `<p>DriveChanged(driveId)` subscription delivering `DriveDelta` over the common
  `DriveFile` value type with its `affordances` (`delete`, `rename`, `move`,
  `download` — denied `FILE_NOT_READY` until the file is `READY`) computed from
  the host gate and `protected`. `MoveFolder`, `DeleteFolder` and
  `delete_drive` run on the engine's bulk pipeline (set-based statements, one
  blob release per file, a projector reset past `DriveHost::BULK_RESET_THRESHOLD`)
  so a folder or a drive of any size can be moved or deleted.
- Abandoned uploads: a scheduled `upload-deadline` reaction deletes a file
  still `PENDING` past the host's `upload_window` (releasing its blob for the
  engine reaper); a post-upload policy on the `drive_source` kind impacts the
  file (`SourceAvailable`) when the reaper promotes its object.
- Value objects and bounds: `MediaType` (`type/subtype` token pair, ≤ 255
  bytes, `INVALID_MEDIA_TYPE`), whole path ≤ 1024 bytes and no padded segment
  (`INVALID_PATH`), `ByteCount` (64-bit sizes on the wire), matching `CHECK`
  constraints in the migration. `set_metadata` gives the host a write path for
  the file's free JSON. The library refuses to register on a host that
  configured no object storage.
- Pages, images and the runner surface. `drive.file_page` (pk `(file_id,
  number)`, origin `runner | regenerated | edited`, `updated_by/at`) and
  `drive.file_image` (pk `(file_id, name)`, its own verified blob of kind
  `drive_image` with policy + post-upload policy, `page` derived from the
  page-scoped name `p{page:03}-img{n:02}.{ext}`); `drive.file` gains
  `summary`, `page_count`, `estimated_tokens` (written together by the
  indexer's report). Roots under the host prefix, gated on a
  `Passport::Service` carrying `DriveHost::RUNNER_SCOPE`
  (`RUNNER_SCOPE_REQUIRED` otherwise, and a runner sees nothing through the
  drive views): `<p>RunnerContext(fileId)` (fresh presigned GET on the source,
  media type, name, page count, pages, image names),
  `<p>RunnerRequestImageUpload(fileId, jobId, name, mediaType, size, sha256)`
  (an existing name is replaced and its old object released),
  `<p>RunnerReport(fileId, jobId, pages, origin, summary, pageCount,
  estimatedTokens, done)` (page batches upsert by number; a `REGENERATED` page
  drops the images it no longer references; the indexer triple moves together;
  `done` calls the `report_done` seam that milestone 4 turns into
  `job.finish`). `jobId` is validated against the host seam
  `DriveHost::active_job(&FileRow)` (`JOB_NOT_ACTIVE`) until the File carries
  its own `job_id`. User gesture `<p>EditPage(fileId, number, markdown)`
  (origin `EDITED`, `READY` only, affordance `editPage`); `<p>FileAccess(fileId,
  name)` answers the image GET (inline). `DriveFile` carries `pages`, `images`,
  `summary`, `pageCount`, `estimatedTokens`; deleting a file drops its pages and
  images and releases every image blob. Eight more e2e scenarios: runner
  context with a fresh GET, scope refusals (human, unscoped service), wrong
  job refused, image round trip (verified, wrong checksum refused, unknown
  name) and naming refusals, report batches with an idempotent replay and the
  indexer triple, page edit (and its refusals), page regeneration replacing
  images by name, cascade releasing image blobs.
- Reason codes: `DRIVE_NOT_FOUND`, `FILE_NOT_FOUND`, `FOLDER_NOT_FOUND`,
  `FILE_PROTECTED`, `FILE_NOT_PENDING`, `FILE_NOT_READY`, `FILE_TOO_LARGE`,
  `UPLOAD_NOT_LANDED`, `INVALID_SHA256`, `INVALID_MEDIA_TYPE`, `INVALID_PATH`,
  `INVALID_NAME`, `NAME_TAKEN`, `FOLDER_INTO_ITSELF`, `NOTHING_TO_CHANGE`,
  `RUNNER_SCOPE_REQUIRED`, `JOB_NOT_ACTIVE`, `SOURCE_NOT_AVAILABLE`,
  `INVALID_IMAGE_NAME`, `INVALID_PAGE`, `INVALID_PAGE_ORIGIN`, `PAGE_NOT_FOUND`,
  `INDEXER_FIELDS_TOGETHER`, plus
  the engine's `KEY_REUSED` and the host's own codes through the gate and the
  hooks.
- Example host: `workspaceCreate` / `workspaceDelete` wrap `create_drive` /
  `delete_drive`, `workspaceTransfer` moves the drive's visibility,
  `workspaceProtectFile` wraps `set_protected`; the host gate refuses one media
  type it cannot render and the hooks log every folder gesture (and refuse the
  `forbidden` prefix). Twenty-four e2e scenarios over real PostgreSQL, NATS and
  MinIO: verified round trip, wrong checksum refused by storage, commit in the
  pending window, abandoned upload reaped (never posted, and landed but never
  committed — with the object's deletion), sibling collision, normalization
  refusals, folder move and delete with the hooks in the transaction, the bulk
  path past the reset threshold (move, folder delete, drive delete), rename and
  move, `protected`, metadata, cascade releases blobs, visibility loss delivers
  `Remove` and the old session then stays silent, the outsider is refused every
  gesture, cross-drive move, two concurrent uploads of one name and two
  concurrent renames onto one name, a redelivered upload deadline, a fresh
  subscription after mutations.
