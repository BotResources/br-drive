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
   may neither rename, move nor delete; `br_drive::drive_of` answers which drive
   a file belongs to.

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
| `<p>RequestUpload(fileId, driveId, path, name, mediaType, size, sha256): JSON!` | gate `CreateFile` → uniqueness (` (1)`, ` (2)` before the extension) → verified presigned POST pinning the exact size and SHA-256 → the file row `PENDING`. Returns `{ fileId, url, fields }`: the front form-POSTs the bytes to `url` with `fields`. One transaction. |
| `<p>CommitUpload(fileId): MutationAck!` | a live storage HEAD in the pending window: `UPLOAD_NOT_LANDED` if the object is absent, `UPLOAD_MISMATCH` if it is not the pinned bytes; else `READY` (no ruleset yet). A second commit is `FILE_NOT_PENDING`. |
| `<p>UpdateFile(fileId, name?, path?, driveId?): MutationAck!` | rename, move, or move to another drive of the same host; `NAME_TAKEN` on collision, `NOTHING_TO_CHANGE` when nothing differs, `FILE_PROTECTED` on a protected file. |
| `<p>DeleteFile(fileId): MutationAck!` | cascade; the source blob is released in the same transaction. |
| `<p>MoveFolder(driveId, oldPrefix, newPrefix): MutationAck!` | one bulk `UPDATE` on the prefix (the rows are locked first), then `folder_moved`; `FOLDER_NOT_FOUND`, `FOLDER_INTO_ITSELF`, `NAME_TAKEN` (refused as a whole), `FILE_PROTECTED` if any file under the prefix is protected, `INVALID_PATH` for the root. |
| `<p>DeleteFolder(driveId, prefix): MutationAck!` | deletes every file under the prefix (each releases its blob), then `folder_deleted`. |
| `<p>File(fileId): DriveFile` | the file as the caller sees it, or `null`. |
| `<p>DriveFiles(driveId): [DriveFile!]!` | the drive's files as the caller sees them (the tree is a path prefix; empty folders do not exist). |
| `<p>FileAccess(fileId, name: String): String` | a short-lived presigned GET on the source (attachment); `null` until the object is landed **and** promoted by the engine reaper (a verified blob is never downloadable in the pending window), or when the caller cannot see the file; a coded error when the `download` affordance is denied. `name` is reserved for the extracted images (milestone 3) and answers `null` today. |
| `<p>DriveChanged(driveId): DriveDelta!` | the engine snapshot on connect (`DriveReset`), then `DriveUpsert` / `DriveRemove` on the contiguous revision. |

`DriveFile`: `id`, `driveId`, `path` (normalized, `""` = root), `name`,
`protected`, `mediaType`, `sizeBytes`, `sha256` (hex), `processingState`
(`PENDING | PROCESSING | READY | FAILED`), `processingError`, `metadata`
(JSON), `createdBy`, `createdAt`, `updatedAt`, `affordances`.

`DriveDelta` is the engine's delta union: `DriveReset { revision, views }`,
`DriveUpsert { revision, view, cause }`, `DriveRemove { revision, projector,
key, cause }`, plus the lane notices. `cause` is one of the library's file
causes (`UploadRequested`, `UploadCommitted`, `UploadAbandoned`,
`SourceAvailable`, `Renamed`, `Moved { from_drive }`, `FolderMoved`,
`ProtectionChanged { protected }`, `Deleted`, `FolderDeleted`, `DriveDeleted`)
on a delta the engine attributes to an impact; a key that **enters or leaves**
a live session's window (a created or deleted file, a drive gained or lost)
is delivered by the engine's window repopulation and carries no cause in engine
0.3.0.

Abandoned uploads: `RequestUpload` schedules an `upload-deadline` reaction at
`now + upload_window`; a file still `PENDING` then is deleted (its blob is
released and the engine reaper removes the object). A file whose object landed
but was never committed is deleted the same way.

Paths: `/`-separated segments, no leading or trailing slash (both are
normalized away), no empty, `.` or `..` segment, no control character or
backslash, segment ≤ 255 bytes, `""` is the root. Names obey the segment rules
and carry no `/`. Anything else is `INVALID_PATH` / `INVALID_NAME`.

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
