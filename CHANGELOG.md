# Changelog

All notable changes to `br-drive` are documented here. The whole workspace
ships **one version**: every crate inherits `version.workspace = true`, and a
single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## Unreleased

### Fixed

- A processing chain of more than one step reaches `READY` against the real
  Jobs service: no step names the previous step's job as `parent_job_id` any
  more. Jobs refuses a terminal parent (`parent_job_terminal`), and the chain
  only advances once the previous job completed, so every second step was
  rejected and its file failed; with a live parent the step would have been
  owned by the parent's runner instead of the host. The chain is a host-side
  sequence correlated by the file id.
- A `creation_rejected` `duplicate_active_entity` no longer leaves the file
  unprocessable: the live job Jobs names (`params.activeJobId`, checked
  against the file and host the rejection names) is kept on the file
  (`drive.file.stray_job_id`) and cancelled at once, and every later launch
  cancels it again before asking for its own job, until Jobs queues a job of
  the file. When the job named is one the library already cancelled (the
  cancel and the create crossed on Jobs' separate consumers), the step waits
  and relaunches instead of failing. `DeleteFile`, `DeleteFolder` and
  `delete_drive` cancel it too; an erase in `Delete` mode cannot (no outbound
  identity in the erase pipeline).
- `job.create` carries only what Jobs accepts: `RequestUpload` refuses a file
  id that is not a UUIDv7 (`INVALID_FILE_ID`) — Jobs refuses a non-v7 source
  entity, which would fail every chain of the file — and `triggered_by` is
  trimmed, a blank display name sent as anonymous, a long one cut at 512
  characters, an erased (nil) initiator not named at all.
- A chain fired before the host's first runner-type catalogue scan (a fresh
  database) is deferred instead of failing the file `catalogue_not_watched`:
  the file waits in its step and a `launch-retry` message asks again after
  `LAUNCH_RETRY_AFTER` (5 s), backing off to `LAUNCH_RETRY_CAP` (5 min), one
  warning at the first deferral. Saving a rule before that scan is accepted,
  every step reported in `unknownRunnerTypes`, instead of refused with
  `CATALOGUE_NOT_WATCHED`. Both codes stay exported, deprecated, no longer
  raised.
- The runner source presign is scoped to the one file the job names, while
  that job is the file's active one, instead of enumerating every file with a
  live job on each `RunnerContext` call; the population re-checks the active
  job with the same rule as the runner roots.
- Image names are no longer capped at page 999 and image 99: the page and
  index widths of `p{page:03}-img{n:02}.{ext}` are minimums (`p1000-img100.png`
  is accepted), a number is never padded wider than it needs, an index is
  never zero.

### Added

- `DriveHost::STEP_TIMEOUT` (default 72 h, Jobs' longest run; checked at
  registration): a step silent that long — measured from its last sign of
  life: entry, run start, plan, step, runner report — has its job cancelled
  and its file lands `FAILED` `timed_out` (`br_drive::TIMED_OUT`). Jobs never
  fails a job no live runner picks up, so this is the way out for a runner
  type with no live instance. One `step-deadline` message per step on a
  `{service}-drive-step-deadline` durable, rescheduled when the step showed
  life since.
- Migration `9121000006`: `drive.file.step_entered_at` (the running step's
  identity; the deadline and the deferred launch key on it),
  `drive.file.step_alive_at` (its last sign of life) and
  `drive.file.stray_job_id`.
- The example host's Jobs stand-in judges every `job.create` the way Jobs
  does, in Jobs' order — the inputs `svc-jobs` refuses before the domain
  (non-v7 ids, a blank or over-long display name, a source not named by its
  producer), a reused id, a second live job on a source entity, a
  self-named, terminal, deleted or unknown parent — and answers
  `creation_rejected` itself; it publishes `cancelled` for a live job it
  cancels, and a create a scenario awaits fails the scenario when refused.
  Regression scenarios for the double; new scenarios for the step timeout, the duplicate-job trap
  and the deferred launch. The example host can boot without its catalogue
  watch (`BootOptions::watch_catalogue`) and start it later.

### Changed

- `FileCause` is `#[non_exhaustive]`; new variant `LaunchDeferred { step }`.
- A chain step never names a `parent_job_id` (see Fixed).
- Image names accept wider page and index numbers (see Fixed).

### Upgrading from 0.1

- Migration `9121000006` adds nullable columns only. Files already
  `PROCESSING` when it is applied carry no step clock and get no deadline:
  let them finish or delete them.
- `DriveHost::STEP_TIMEOUT` is new with a 72 h default; a host that wants its
  users to learn sooner that no runner picked a file up lowers it.
- A host that matched `CATALOGUE_NOT_WATCHED` keeps compiling (deprecated);
  nothing raises it any more.

## 0.1.0 — 2026-09-23

The first release: milestones 1 to 5 on engine `v0.3.0`.

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
  the host gate and `protected` (since milestone 3 the union is `DriveView =
  DriveFile | DrivePage` and the file list never carries the pages). `MoveFolder`, `DeleteFolder` and
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
  page-scoped name `p{page:03}-img{n:02}.{ext}`, plus the pending replacement's
  facts and `landed_at`); `drive.file` gains `summary`, `page_count`,
  `estimated_tokens` (written together by the indexer's report). The rendition
  has its own projector `drive_pages` keyed by `(file_id, number)`: view
  `DrivePage { fileId, number, markdown, origin, updatedBy, updatedAt,
  affordances { editPage } }`, query `<p>Pages(fileId)`, subscription
  `<p>FilePages(fileId)` (a Reset with the file's pages, then one upsert or
  remove per page; gated on `ReadFile`, closed by one Remove per page on a
  visibility loss) and its own member in the `DriveDelta` union; the File
  aggregate never loads its rendition, and a report or a page edit impacts
  page keys, never the file row (the file is impacted only when the indexer
  triple or the image list changes). Roots under the host prefix, gated on a
  `Passport::Service` carrying `DriveHost::RUNNER_SCOPE`
  (`RUNNER_SCOPE_REQUIRED` otherwise, and a runner sees nothing through the
  drive views) and on the file's active job (`JOB_NOT_ACTIVE`):
  `<p>RunnerContext(fileId, jobId)` (fresh presigned GET on the source, media
  type, name, page count, summary, pages and image names read directly by file
  id), `<p>RunnerRequestImageUpload(fileId, jobId, name, mediaType, size,
  sha256)` (an existing name is replaced only when the new object lands — the
  old image stays readable until then and is released in the landing's
  transaction; a re-request while the upload is in flight is
  `IMAGE_UPLOAD_PENDING`), `<p>RunnerReport(fileId, jobId, pages, origin,
  summary, pageCount, estimatedTokens, done)` (batches of at most
  `MAX_REPORT_PAGES` upsert by number; a `REGENERATED` page drops the images
  its markdown no longer references, matched on the whole name; the indexer
  triple moves together and is refused negative; an empty report is
  `NOTHING_TO_CHANGE` unless `done`; `done` calls the crate-private
  `report_done` seam that milestone 4 turns into `job.finish`). `jobId` is
  validated against the host seam `DriveHost::active_job(&FileRow)` until the
  File carries its own `job_id`. User gesture `<p>EditPage(fileId, number,
  markdown)` (origin `EDITED`, `READY` only, affordance `editPage`);
  `<p>FileAccess(fileId, name)` answers the image GET (inline; `null` for a
  caller who cannot read the file). Deleting a file — per row, per folder or
  with its drive — drops its pages and images and releases every image blob
  set-based in the same transaction. Fourteen more e2e scenarios: runner
  context with a fresh GET, `SOURCE_NOT_AVAILABLE` before the reaper's
  promotion, scope refusals (human, unscoped service), a runner holding the
  scope but not the file's job refused on every root, image round trip
  (verified, wrong checksum refused by storage, unknown name, non-owner and
  runner get `null`, `IMAGE_MAX_BYTES` → `FILE_TOO_LARGE`) and naming
  refusals, image replacement (old object readable until the new one lands,
  failed replacement changes nothing, re-request refused with one blob),
  report batches with an idempotent replay, the indexer triple and every
  refusal, two concurrent batches on one file, page edit delivering one page
  upsert and no file upsert, a 300-page report that never rewrites the file
  row, a page window that follows one file and closes on a visibility loss,
  page regeneration replacing images by name, per-row and per-folder deletes
  releasing image blobs.
- Processing rules and the chain over Jobs (milestone 4). `drive.ruleset`
  (`id`, `name` unique case-insensitive, `trigger` `upload | reprocess |
  regenerate_page`, `media_types text[]` of `type/subtype`, `type/*` or `*`,
  `steps jsonb` of `{ runnerType, options }`, `is_default`), the roots
  `<p>Rulesets` / `<p>CreateRuleset` / `<p>UpdateRuleset` / `<p>DeleteRuleset`
  gated by the host's `ManageRulesets` / `ReadRulesets`, one default per
  (trigger, pattern) enforced at save (`DEFAULT_ALREADY_SET`), the unknown
  runner types of a saved rule answered as a warning, precedence exact >
  `type/*` > `*`. `drive.file` gains `ruleset_id`, the `steps` snapshot,
  `step_index` / `step_count` / `step_runner_type`, `job_id` (unique while
  set), `plan`, `progress_index` / `progress_label` / `progress_at` and the
  trigger's principal; `DriveFile` carries `rulesetId`, `steps` and
  `progress { stepIndex, stepCount, runnerType, plan, currentIndex,
  currentLabel, at }`. `CommitUpload(fileId, rulesetId?)` starts the `upload`
  chain or lands `READY` when no rule matches; `<p>Process(fileId,
  rulesetId?)` (wipes pages, images and the indexer triple at chain start) and
  `<p>RegeneratePage(fileId, number, comment?, rulesetId?)` (page and comment
  merged into the first step's options). The chain checks the runner type in
  `drive.known_runner_type` (`runner_type_unavailable` before any job),
  stages `integration.cmd.jobs.job.create.v1` with the documented config
  (`host`, `file_id`, `job_id`, the three root names, `step`, `options`;
  `source_bc` / `source_entity_id` = the file, `triggered_by` from
  `DriveHost::display_name`), consumes the eight `integration.evt.jobs.job.*.v1`
  facts filtered on the File's `job_id` (unknown job → acknowledged no-op;
  `completed` → next step or `READY`; `creation_rejected` / `failed` →
  `FAILED` with the reason code; foreign `cancelled` → `FAILED cancelled`),
  stages `job.finish.v2` on the report's `done` and `job.cancel.v2` when a
  `PROCESSING` file is deleted (per row, per folder, with its drive).
  `br_drive::watch_runner_types` mirrors the Jobs catalogue (boot scan + KV
  watch, tolerant). The `active_job` host seam and the example's metadata stub
  are gone; `DriveHost::display_name` is new; `EditPage` / `RegeneratePage`
  during a chain are `FILE_PROCESSING`; a report on a file without a running
  job is `JOB_NOT_ACTIVE`; the runner source population is restricted to the
  files with a live job. Pinned `contract-jobs` 0.5.0. Ten more e2e scenarios
  with a Jobs stand-in publishing the real DTOs on the real subjects: rules
  empty → `READY` and no job; ruleset CRUD, gates, validation, name and
  default uniqueness, save-time warning; the full chain (queued → plan → step
  → report → finish → next step → `READY`) with every `job.create` field
  asserted; variant by id, catch-all, mismatch; unknown / deprecated runner
  type; failure and rejection with reason codes, reprocess wiping the
  rendition; foreign cancel vs own cancel; a rule edited or deleted mid-chain,
  non-retroactivity; every fact replayed idempotent; the processing guards.
  Review round:
  every durable the library binds is namespaced by `DriveHost::SERVICE`
  (`{service}-drive-…`, proven by a two-host scenario on one broker); the
  config root names follow the engine's camel-casing of the prefix; `Process`
  falls back to the file's own `steps` snapshot when no `reprocess` rule
  matches; the catalogue watch records its boot scan and a host that never
  started it is refused `CATALOGUE_NOT_WATCHED` on rule saves and fails chains
  `catalogue_not_watched`; a catalogue entry of another wire version is
  treated as unknown; the report's `done_at` gates the chain's advance and a
  `completed` fact that arrives first waits for the report; a step cursor
  moves forward by index or by a newer start instant; a malformed snapshot
  is read as absent rather than breaking the file's view.
- Labels and erase (milestone 5). `drive.label` (`name` ≤ 100 characters,
  trimmed, unique per host case-insensitive; `color` `#rrggbb` lowercase hex;
  `description` default `''`, at most 1 KiB) and `drive.file_label`; roots
  `<p>Labels`,
  `<p>CreateLabel` / `<p>UpdateLabel` / `<p>DeleteLabel` (gate `ManageLabels`)
  and `<p>SetFileLabels(fileId, labelIds)` (gate `SetFileLabels { file }`,
  target set, idempotent); `DriveFile.labelIds` computed by label name and the
  `setLabels` affordance; `DeleteLabel` on the bulk pipeline detaches its
  files (`LabelsChanged { detached }` per file, a projector reset past the
  threshold); the name check is serialized like the rulesets'; labels travel
  with a file across the host's drives; `<p>LabelsChanged` and
  `<p>RulesetsChanged` subscriptions (the
  union is `DriveView = DriveFile | DrivePage | DriveLabel | DriveRuleset`,
  a rule save carries `Saved { unknown_runner_types }` as its cause). The
  engine's `Erasable` for the library's rows, driven by the engine's erase
  pipeline in `DriveHost::erase_mode` — `Anonymise` rewrites every id the
  person left to `REDACTED_PERSON`, `Delete` removes the person's files
  (cascade, objects purged) and anonymises the rest;
  a replayed erase is absorbed. Review follow-ups: `ruleset` and `processing`
  split by responsibility, the last data-path `expect` removed, the catalogue
  stamps taken from the database clock, `select_ruleset` reads one trigger's
  defaults. Five more scenarios (58 in all): label catalogue live, a file's
  label set, anonymise, delete on the second host, the live rule table.
- The workspace MSRV is Rust 1.94: the floor of the pinned `contract-jobs`
  0.5.0, ahead of the engine's own 1.89.
- Reason codes: `DRIVE_NOT_FOUND`, `FILE_NOT_FOUND`, `FOLDER_NOT_FOUND`,
  `FILE_PROTECTED`, `FILE_NOT_PENDING`, `FILE_NOT_READY`, `FILE_TOO_LARGE`,
  `UPLOAD_NOT_LANDED`, `INVALID_SHA256`, `INVALID_MEDIA_TYPE`, `INVALID_PATH`,
  `INVALID_NAME`, `NAME_TAKEN`, `FOLDER_INTO_ITSELF`, `NOTHING_TO_CHANGE`,
  `RUNNER_SCOPE_REQUIRED`, `JOB_NOT_ACTIVE`, `SOURCE_NOT_AVAILABLE`,
  `INVALID_IMAGE_NAME`, `INVALID_PAGE`, `INVALID_PAGE_ORIGIN`, `PAGE_NOT_FOUND`,
  `INDEXER_FIELDS_TOGETHER`, `INVALID_INDEXER_VALUE`, `BATCH_TOO_LARGE`,
  `IMAGE_UPLOAD_PENDING`, `RULESET_NOT_FOUND`, `RULESET_NAME_TAKEN`,
  `INVALID_RULESET`, `DEFAULT_ALREADY_SET`, `RULESET_MISMATCH`,
  `NO_RULESET_MATCHES`, `RUNNER_TYPE_UNAVAILABLE`, `FILE_PROCESSING`,
  `CATALOGUE_NOT_WATCHED`, `LABEL_NOT_FOUND`, `LABEL_NAME_TAKEN`,
  `INVALID_LABEL`, plus
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
