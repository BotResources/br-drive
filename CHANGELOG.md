# Changelog

All notable changes to `br-drive` are documented here. The whole workspace
ships **one version**: every crate inherits `version.workspace = true`, and a
single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## Unreleased

Nothing yet.

## 0.8.2 — 2026-11-05

A chain's first step is asked of Jobs once the engine promoted the file's
source, no longer at the commit. No signature changes; a host on 0.8.1
upgrades by bumping the pin and applying the new migration (the library's
`migrate` does). Engine and `contract-jobs` pins are unchanged.

### Fixed

- **A runner no longer meets `SOURCE_NOT_AVAILABLE` on its first call.** The
  commit (on a host that processes on commit) checked the source with a live
  storage HEAD and staged the first step's `job.create` in its own
  transaction, while the engine's blob row was still `pending`: the engine
  promotes it only in its blob reaper (once per reaper interval, 60 s by
  default) and serves no download URL for a verified source before that. The
  runner, called a fraction of a second after the commit, was refused
  `SOURCE_NOT_AVAILABLE` until the reaper's next sweep and leaned on Jobs'
  retries (observed 10–42 s on a real deployment). Now a chain started on a
  source still pending **awaits its promotion**: the gesture records
  everything it recorded before — the rule (the commit's choice rules are
  unchanged), the `steps` snapshot, the initiator's Passport, the file
  `PROCESSING`, `ChainStarted` — mints the first step's job id and records
  `SourceAwaited` instead of `JobCreated`; the source's post-upload policy,
  in the promotion's transaction, commands the new `source-promoted`
  self-command, whose reaction stages the `job.create.v3` with the minted id
  (`JobCreated`). The runner's first `<p>RunnerContext` call is served.
  Behaviour is unchanged when the reaper already promoted the source (a late
  commit, a reprocess, a regeneration): the job is created in the gesture.
  The same holds for `ProcessFile` right after a commit on a two-gesture host.
- Between the gesture and the promotion the file is `PROCESSING`, `progress`
  at step 0 with nothing run, `lastJob` the minted job with an
  `awaiting_source` entry (`FileJob::awaiting_source()`), and no active job
  (`FileRow::active_job()` is `None`; the minted id answers `JOB_NOT_ACTIVE`
  to a runner). A `<p>CancelProcessing` or a `freeze_drive` drops the
  awaiting step at once (`AwaitedStepDropped`, the file `FAILED`
  `cancelled`) and sends Jobs nothing; a deleted file leaves the promotion
  nothing to start; an initiator erased meanwhile drops the step
  `passport_missing` at the promotion. A redelivered promotion creates the
  step once.

### Changed

- **`FileRow::active_job()` answers `None` while the first step awaits the
  source**, as it does while a next step is held (0.8.1).
- **A host whose `record_facts` refuses a step's `JobCreated` refuses it in
  the promotion's reaction** when the step awaited its source, not in the
  gesture: the gesture is acked, the reaction is retried, and the file stays
  `PROCESSING` until a user cancels it.
- `SOURCE_NOT_AVAILABLE` stays as a defensive refusal of `<p>RunnerContext`
  (a source the engine cannot serve); it is no longer the normal path, and
  the host contract no longer documents "retry until promoted".

### Added

- **Migration `9121000016_processing_awaiting_source`**: `drive.file_processing
  .awaiting_source` (boolean, default `false`, only while `processing`).
  Additive; a 0.8.1 binary ignores it and never creates an awaiting step.
  **Upgrade with no mixed 0.8.1 / 0.8.2 consumers** — a `Recreate` rollout,
  or one replica during the upgrade — and let awaiting steps drain (none
  waits longer than one reaper interval) before rolling back. A file
  stranded all the same is released by the user's `<p>CancelProcessing` on
  0.8.2.
- **Self-command `source-promoted`** (durable
  `{SERVICE}-drive-source-promoted`), the library's twelfth reaction; it
  arrives as a service actor, like `image-landed`.
- **Facts** on `drive_file_processing`: `SourceAwaited { step_index,
  runner_type, job_id }` and `AwaitedStepDropped { step_index, runner_type,
  job_id, reason_code }`, appended after `NextStepHeld` (`ProcessingEvent` is
  `#[non_exhaustive]`).
- **`FileJob::awaiting_source()`** and the job-event kind
  `awaiting_source`.
- e2e `source_promotion`: with the reaper held off the source (a row lock on
  its blob row, anchored on the reaper waiting on it), no `job.create` before
  the promotion and the job created with the minted id once it lands, the
  runner served on its first call; a replayed promotion creates the step
  once; a commit on a source already promoted creates the job in the commit;
  a `ProcessFile` right after a commit awaits too; a cancel, a freeze, a
  delete and an erasure while the step awaits; a rule deleted meanwhile does
  not change the chain the commit decided. `upload_processed` and the
  `commit_processing` scenarios now wait for the promotion first, so they
  keep proving the in-gesture path whatever the reaper's timing; the runner
  scenario that expected `SOURCE_NOT_AVAILABLE` until the promotion now
  expects no job and `JOB_NOT_ACTIVE`.

## 0.8.1 — 2026-11-04

A chain's next step is created once Jobs ended the previous one, no longer
beside the previous step's `job.finish`. No signature changes; a host on 0.8.0
upgrades by bumping the pin and applying the new migration (the library's
`migrate` does). Engine and `contract-jobs` pins are unchanged.

### Fixed

- **A multi-step chain no longer fails after its first step with
  `duplicate_active_entity`.** The runner's final report staged, in one
  transaction, the step's `job.finish` and the next step's `job.create`, both
  on the file as Jobs' source entity. Jobs consumes the two on separate
  durables, in no order, and holds one live job per source entity: a create
  judged before the finish was applied was refused `creation_rejected`
  `duplicate_active_entity`, and the file landed `FAILED` after step one
  (observed with the create 4 ms ahead of the finish). Single-step chains
  were never affected. Now the final report of a step that has a next one
  stages the `job.finish`, decides the next step (its runner type, the
  chain's Passport, a cancel already pending) and **mints and holds** its job
  id, the file still `PROCESSING`; the `job.create` is staged when Jobs' own
  end of the previous job reaches the library — its `completed`, or a
  `failed` / `cancelled` / `creation_rejected` Jobs settled before the
  finish (still recorded as `JobFactIgnored` `job_already_ended`: the report
  was the job's end) — with the minted id, and the step becomes the file's
  running job as before (`JobCreated`, `ProcessingStarted`). A redelivered
  end finds the step already created (`not_the_current_job`): the step is
  created once, and Jobs is idempotent on the id.
- While the next step is held, the job reported done is no longer the
  file's active job: its runner's calls answer `JOB_NOT_ACTIVE` and its late
  progress facts are ignored; `progress` names the next step with nothing
  run yet (`FileJob::reported_done()` tells the held state; the job's
  `events` carry a `reported_done` entry). A user's `<p>CancelProcessing`
  skips a held step at once (`StepSkipped`, the file `FAILED` `cancelled`;
  `job.cancel.v2` is still staged for the job reported done, harmless on a
  settled job), so a Jobs end lost on its way can never strand the file in
  `PROCESSING`; Jobs' later end of that job only confirms. A Passport
  erased meanwhile abandons the step at release (`StepAbandoned`
  `passport_missing`). `freeze_drive` skips a held step at once too (no
  `job.cancel`: nothing runs).

### Changed

- **`FileRow::active_job()` answers `None` while a next step is held**: the
  job reported done no longer runs, though the file is `PROCESSING`. A host
  that reads "is the file processing" from `active_job()` reads
  `processing_state()` instead, or `last_job()` with
  `FileJob::reported_done()`.

### Added

- **Migration `9121000015_processing_next_job`**: a nullable
  `drive.file_processing.next_job_id` (uuid), the held next step's job id.
  Additive; a 0.8.0 binary ignores it, and acks Jobs' `completed` of a job
  reported done as information: a step it finds held is never created.
  **Upgrade with no mixed 0.8.0 / 0.8.1 consumers** — a `Recreate` rollout,
  or one replica during the upgrade — and let held steps drain (they last as
  long as Jobs takes to answer a `job.finish`) before rolling back. A file
  stranded all the same is released by the user's `<p>CancelProcessing`.
- **Fact** `NextStepHeld { step_index, runner_type, job_id, after_job_id }`
  on `drive_file_processing`, between `JobReportedDone` and the next
  `JobCreated` (or `StepSkipped` / `StepAbandoned`). `ProcessingEvent` is
  `#[non_exhaustive]`.
- **`FileJob::reported_done()`**.
- e2e `chain_handoff`: no `job.create` before Jobs' `completed` and the chain
  lands `READY`; a replayed `completed` creates the step once; a cancel
  between the report and `completed` skips the step at once; a Jobs
  `failed` of the job reported done still hands off, and a `failed` after
  the `completed` that released the step creates nothing more; a freeze or
  a delete while the step is held, then `completed`, creates nothing; a
  `ProcessFile` while it is held is refused `FILE_PROCESSING`. `runner_end` and `renditions` complete
  the first job before awaiting the second step's create.

## 0.8.0 — 2026-11-03

File renditions: a runner hands back a binary file derived from the source (a
database dump converted into an analytic `.duckdb` file, later a Parquet export
or a PDF), stored in the host's object storage beside the source, owned by the
file, read later by a runner or a reader. The markdown pages are the file's
markdown rendition; every other rendition is a file rendition, an object in
storage. The library stays content-blind: it stores the bytes and their
metadata, never reads them.

### Upgrading from 0.7

- Bump the br-drive pin; the engine (`br-service-engine/v0.4.2`) and
  `contract-jobs` (`v0.7.0`) pins are unchanged.
- The library registers a third blob kind, `drive_rendition`, and an eleventh
  reaction, the `rendition-landed` self-command (durable
  `{SERVICE}-drive-rendition-landed`), which arrives as a service actor like
  `image-landed`.
- A host that builds `DriveFile`, `RunnerContext` or `RootNames`
  values by hand (a test, a mapping) adds the new `renditions` /
  `rendition_upload_root` fields; a host matching `FileEvent` / `FileCause`
  exhaustively cannot (both are `#[non_exhaustive]`).

### Added

- **Migration `9121000014_file_rendition`**: the table `drive.file_rendition`,
  one row per `(file_id, name)` — shaped like `drive.file_image` without
  `page`, plus `kind` (and `pending_kind`, the kind of a replacement in
  flight). The row references `drive.file` `ON DELETE CASCADE`. Additive: no
  existing table or row changes; a 0.7.x binary ignores it.
- **`<p>RunnerRequestRenditionUpload(fileId, jobId, name, kind, mediaType,
  size: ByteCount, sha256): UploadTicket!`**: the runner scope, then the file's
  active job (`JOB_NOT_ACTIVE`), like `<p>RunnerRequestImageUpload`; a
  verified presigned POST for a `drive_rendition` blob, `FILE_TOO_LARGE` past
  the new `DriveHost::RENDITION_MAX_BYTES` (default 5 GiB, the AWS S3 cap on a single presigned POST). `name` is
  `[a-z0-9][a-z0-9._-]*`, 1–64 bytes (`INVALID_RENDITION_NAME`); `kind` is
  `[a-z][a-z0-9_]*`, up to 32 bytes (`INVALID_RENDITION_KIND`). A name is
  replaced only when its new object lands — the old object and kind stay
  current until then, a failed upload changes nothing, the old object is
  released in the landing's transaction — and a re-request while an upload is
  in flight is `RENDITION_UPLOAD_PENDING`.
- **`RunnerContext.renditions[] { name, kind, mediaType, sizeBytes, url }`**:
  the file's landed renditions, each with a fresh presigned GET at every call,
  so a later step of the chain reads what an earlier step produced; a
  rendition replaced while it is presigned is re-read and its current object
  signed, never left out. Gated like
  the source (`RunnerRenditions`, the runner's own file while its job is the
  active one).
- **`DriveFile.renditions[] { name, kind, mediaType, sizeBytes, landedAt }`**
  (no bytes, no URL) and **`<p>RenditionAccess(fileId, name): String`**, a
  short-lived presigned GET (attachment) behind the same gate as
  `<p>FileAccess`: `null` when the caller cannot see the file, for an unknown
  name and until the object landed; a coded refusal when the `download`
  affordance is denied.
- **`DriveHost::RENDITION_MAX_BYTES`** (5 GiB, the AWS S3 cap on a single presigned POST) and
  **`DriveHost::RENDITION_ORPHAN_AFTER`** (24 h), the `BlobPolicy` of the new
  kind.
- **Facts** on the file (`drive_file`): `RenditionUploadTicketIssued { name,
  rendition_kind }`, `RenditionAvailable { name, rendition_kind }` (first
  landing), `RenditionReplaced { name, rendition_kind }` (a replacement
  landed). The field is `rendition_kind` because `kind` is the event's tag.
  File causes `RenditionRequested { name }` and `RenditionAvailable { name }`.
- **Job config**: `rendition_upload_root` (`<p>RunnerRequestRenditionUpload`)
  beside the other roots.
- Reason codes `INVALID_RENDITION_NAME`, `INVALID_RENDITION_KIND`,
  `RENDITION_UPLOAD_PENDING`.

### Changed

- **Every path that deletes a file releases its rendition objects
  explicitly**, in its own transaction — `DeleteFile`, `DeleteFolder`,
  `delete_drive`, `delete_drive_in_reaction` and the erase in `Delete` mode
  (purged through the manifest; the erase now locks the person's files
  before reading their object refs, as every other delete path already does,
  so a concurrent upload request or landing cannot slip a ref past the
  purge) — as it releases the image objects: the row's
  `ON DELETE CASCADE` is never seen by the engine's blob accounting. A
  rendition is **never** dropped at a chain's conclusion: a file whose run
  reports zero pages keeps its renditions.
- `file/rendition.rs` is now `file/report.rs` (it validates a runner's page
  reports; "rendition" now names the new concept). Internal, no API change.

## 0.7.0 — 2026-11-02

Breaking: the library now needs a Jobs service serving `contract-jobs` 0.7
(`svc-jobs` ≥ 0.6), which refuses any root job created without a Passport
(`creation_rejected` `passport_required`) — on 0.6.x every file processing was
refused there. A host upgrades by bumping the br-drive pin (same engine tag,
`br-service-engine/v0.4.2`) and `contract-jobs` to `contract-jobs/v0.7.0` if it
pins it; no Rust signature of the host's moves. Its principal's
`Principal::passport()` must answer the caller's own Passport, as the gateway
forwarded it: the library now forwards it to Jobs.

### Upgrading from 0.6

- **Deploy a Jobs service ≥ 0.6 first.** An older one has no
  `job.create.v3` consumer: the first step of every chain would never reach
  it, and its files would stay `PROCESSING` until cancelled.
- **Files processing at the upgrade need a reprocess.** Their chain has no
  stored Passport: the running step's job, created without a Passport, fails
  on the Jobs side (`passport_required`), and a step that ends anyway leaves
  its next one `FAILED` `passport_missing`. Either way the file ends
  `FAILED`; reprocess it once the upgrade is done.
- The Passport is a snapshot taken at the chain's start (scopes, active and
  super-user flags): deactivating a user or revoking a scope does not stop a
  chain already running, unlike an erase.

### Changed

- **Every step's job is created on `integration.cmd.jobs.job.create.v3`**
  (`contract_jobs::command_v3::CreateJobV3`), carrying the Passport of the
  principal whose gesture started the chain — the commit that starts the
  `upload` rule, `ProcessFile`, `RegeneratePage` — so the runner acts as that
  person. `triggered_by` is the Passport's own user (Jobs refuses any other,
  `passport_user_mismatch`), named by the initiator's `display_name` when the
  initiator is that user, and nobody under a service Passport; `parent_job_id`
  stays unset, and the other new fields (`payload`, `mode`, `priority`,
  `initial_job_id`, `caller_address`, `acting_*`) are not set. Through 0.6.x a
  next step whose initiator was unknown named the file's creator: it now
  always names the Passport's user.
- **contract-jobs 0.5.1 → 0.7.0.** `job.finish`, `job.fail`, `job.cancel`
  (v2) and the eight `job.*.v1` facts the library consumes are unchanged:
  Jobs 0.6 still serves them.

### Added

- **Migration `9121000013_processing_passport`**: a nullable JSONB column
  `drive.file_processing.passport`, the Passport the running chain acts
  under, with a check that it is set only while the processing is
  `processing`. Written in the transaction that starts the chain, read back
  for each next step, and set back to `NULL` in the transaction of every end
  of the processing (`READY`, a runner's or Jobs' failure, a cancel, a
  rejection, a drive frozen; the row goes with a deleted file or drive). No
  fact, view, published event or KV entry, nor log holds it; beside the
  column it is written only in the engine's outbox row of each step's
  `job.create.v3`, swept after the engine's message retention (published
  rows about an hour after publication; pending and failed ones longer) and
  not reached by the erase — Jobs keeps its own copy with the job. The check
  is added `NOT VALID` (the column is new and `NULL` everywhere), so the
  migration does not scan the table under its lock; it holds for every
  write. Additive: a 0.6.x binary ignores the column.
- **A chain whose Passport is gone stops visibly.** When a next step is due
  and the stored Passport is missing or no longer reads as a Passport, no job
  is sent: the chain stops with the new processing fact
  `StepAbandoned { step_index, runner_type, after_job_id, reason_code }` and
  the file lands `FAILED` `passport_missing` or `passport_unreadable`
  (`br_drive::PASSPORT_MISSING`, `br_drive::PASSPORT_UNREADABLE`), open to a
  reprocess. A chain already running when the migration lands has no stored
  Passport: it ends this way at its next step.
- **The erase drops a running chain's Passport** of the erased person
  (`Anonymise` and `Delete`) — their own, and one they hold as its
  impersonator (the Passport's `impersonator`, an administrator who borrowed
  the initiator's session): the chain is not carried on as, nor on behalf of,
  someone erased, and ends `FAILED` `passport_missing` at its **next** step;
  the step running at the erase goes on until its runner reports (its job
  already carries the Passport). Through 0.6.x it ran
  on, naming nobody.
- The example host's Jobs double judges creates the way Jobs 0.6 does
  (`job.create.v3` only — a v1 create fails the scenario —,
  `passport_required`, `passport_malformed`, `passport_user_mismatch`), and
  the e2e prove that every step's job carries the initiator's Passport and
  names its user, that the column is filled while `PROCESSING` and `NULL`
  after each kind of end, that no fact records it, that a chain whose
  initiator — or the impersonator of its initiator's session — is erased ends
  `FAILED` `passport_missing` with no job sent, that an impersonated session
  and a service account each start a chain Jobs accepts, and that a chain
  running on a 0.6 database at the upgrade ends `FAILED` `passport_missing`
  at its next step. The double records a command Jobs refuses as a breach of
  its contract (a v1 create, an undecodable command) and fails the scenario
  on its next read, or at its drop, instead of dying silently.

## 0.6.5 — 2026-10-23

Fix only: no API change, the committed SDL did not move, and a host on 0.6.4
upgrades by bumping the br-drive pin alone (same engine tag).

### Fixed

- **`ByteCount` accepts a whole number written as a float.** A gateway that
  re-encodes the integers of a variables object forwards `33` as `33.0`; the
  scalar refused it (`invalid type: floating point 33.0, expected u64`), so an
  upload requested with the size as a variable failed and nothing was stored.
  A whole, non-negative float up to 2^53 is now read as that byte count; a
  fractional, negative or non-finite number is still refused. The value is
  always written back as an integer.

### Security

- **Lockfile moves for three new advisories**: `ctutils` 0.4.5
  (RUSTSEC-2026-0385), `rustls-webpki` 0.103.16 (RUSTSEC-2026-0382) and
  `webpki-roots` 1.0.11 (RUSTSEC-2026-0381). A host updates its own lockfile
  the same way.
## 0.6.4 — 2026-10-20

Engine patch only: no API change, the committed SDL did not move, and a host
on 0.6.3 upgrades by bumping both pins together (br-drive v0.6.4 and
service-engine `br-service-engine/v0.4.2`), moving its `br-rust-common`
crates to their per-crate tags at the same time.

### Changed

- **service-engine v0.4.1 → v0.4.2.** A host pins the same engine tag as the
  library. 0.4.2 fixes, among others, a static-musl binary that failed at
  boot with `SchemaParse "stack limit reached"` once pest ≥ 2.9.3 resolves, a
  passport rejection answered as plain text, a stale principal reused on a
  WebSocket after a revocation, and a displaced pod that kept writing.
- **`br-core-auth` and `br-core-integration` move to their per-crate tags**
  (`br-core-auth/v0.1.0`, `br-core-integration/v0.1.0`), as the engine 0.4.2
  does: `br-rust-common` is no longer released under one `v1.x` tag, and a
  host must pin the same per-crate tags so that the shared types stay one.
- **contract-jobs 0.5.0 → 0.5.1**, the release on the same per-crate tags.
  No br-drive code changes.
- `rustls` moves to 0.23.46 in the lockfile, which clears the
  RUSTSEC-2026-0346, -0347, -0348 and -0349 advisories `cargo deny` reports.

## 0.6.3 — 2026-10-09

Engine patch only: no API change, the committed SDL did not move, and a host
on 0.6.2 upgrades by bumping both pins together (br-drive v0.6.3 and
service-engine v0.4.1).

### Changed

- **service-engine v0.4.0 → v0.4.1.** A host pins the same engine tag as the
  library, and 0.4.1 fixes a led mirror that dropped KV changes with two or
  more pods, signs the blob confirm's checksum header, and adds full scope
  specs. No br-drive code changes.

## 0.6.2 — 2026-10-02

Additive only: no signature changes, the committed SDL did not move, and a
host on 0.6.1 upgrades by bumping the pin. A host that implements nothing
keeps 0.6.1's behaviour; one that implemented `admit_subscription` keeps its
subscriptions refused as before.

### Added

- **`DriveHost::admit(&self) -> Gate`**, the host's caller admission. The
  default admits everyone; a host implements it with the rule its own roots
  apply (a person deactivated or unknown in its roster, a service account on
  a surface meant for people, a person without its access scope), read from
  its own data.
- The example host implements `admit` (a deactivated account is refused
  `ACTIVE_USER_REQUIRED`); e2e prove that every query, mutation and
  subscription root of the slice answers that code at the root, byte-identical
  for an existing and a random id, with nothing recorded and no stream
  opened, that the suite's root list matches the slice's schema, and that the
  runner roots still answer `RUNNER_SCOPE_REQUIRED` to a non-runner, admitted
  or not; unit tests pin the defaults.

### Changed

- **`DriveHost::admit_subscription` is deprecated** in favour of `admit`; its
  default is now `self.admit()`, so a host implements `admit` only and the
  four subscriptions get the same answer as every other root. A host that
  implements it still builds warning-free (implementing a deprecated method
  is not a use of it); only calling it warns.

### Fixed

- **Every root of the drive slice asks the host's admission first.** The
  queries (`<p>File`, `<p>DriveFiles`, `<p>Pages`, `<p>FileAccess`,
  `<p>Labels`, `<p>Rulesets`, `<p>RunnerTypes`), every mutation and the four
  subscriptions call `admit` before any lookup and refuse with the gate's
  reason as `errors[].extensions.code`. Through 0.6.1 only the subscriptions
  asked: a mutation or a read looked the file, folder or drive up first, so a
  caller the host does not admit met `FILE_NOT_FOUND`, `FOLDER_NOT_FOUND` or
  an empty catalogue — an answer depending on whether the id existed, and
  never the host's own code. The four runner roots keep their runner-scope
  check, asked before any lookup, and do not ask `admit`: a host refusing
  service accounts on its human surface keeps its runners working.

## 0.6.1 — 2026-10-01

Additive only: no signature changes, and a host on 0.6.0 upgrades by bumping
the pin. A host that implements nothing keeps 0.6.0's behaviour.

### Added

- **`DriveHost::admit_subscription(&self) -> Gate`**, the host's admission at
  subscription open. The default admits everyone; a host implements it with
  the rule its own subscription roots apply before `service_engine::attach`
  (a person deactivated or unknown in its roster, a service account on a
  surface meant for people), read from its own data.
- The example host refuses a deactivated account `ACTIVE_USER_REQUIRED`;
  e2e for the four subscriptions, refused and admitted, over
  `graphql-transport-ws` and over the `text/event-stream` leg (a new SSE
  reader in the harness).

### Fixed

- **The drive slice's subscriptions honour the host's admission.**
  `<p>DriveChanged`, `<p>FilePages`, `<p>LabelsChanged` and
  `<p>RulesetsChanged` ask `admit_subscription` before attaching: a refused
  principal gets the gate's reason as `errors[].extensions.code` at open and
  no stream. Through 0.6.0 they attached for any authenticated principal, so
  one the host does not admit opened the streams (on an empty `DriveReset`
  when `visible_drives` listed nothing) instead of being refused.

## 0.6.0 — 2026-09-30

Moves the library to `br-service-engine` **v0.4.0** (the engine's breaking
release after 0.3.4); two engine versions cannot coexist in one host, so a
host moves with it. br-drive's GraphQL surface is unchanged (the committed
SDL did not move): the engine forced the window bound, the reads without a
raw pool and the fault rendering below. `cargo semver-checks` against
v0.5.1 reports a major change (`known_runner_types` arity, the
`DriveReactionFault` auto traits); `runner_context`'s parameter type and the
`DriveReactionFault::Store` payload change as well, which it does not see.

### Breaking

- **Engine pin `v0.4.0`.** A host pins `service-engine` at `tag = "v0.4.0"`
  and applies the engine's own *Adopter migration* to its code (pure
  `PassportPrincipal::from_passport(passport)` with database facts in
  `register_principal_fact` loaders, `CohortIndex::keys_in_cohorts(…,
  ceiling)` binding `ceiling.limit()`, a required batched `read_many` and no
  `load` override, `.or_internal(…)?` on a `WindowSpec::view` result, fault
  types that keep their source). The example host shows each one.
  *Migration:* bump both pins; follow the engine CHANGELOG's migration lines.
- **Lists are bounded by the engine's `window_capacity`** (default 10,000):
  `<p>DriveFiles`, `<p>Pages`, `<p>Labels`, `<p>Rulesets`, `<p>RunnerTypes`
  and a new `<p>DriveChanged` / `<p>FilePages` / `<p>LabelsChanged` /
  `<p>RulesetsChanged` session over the capacity are refused
  `WINDOW_TOO_LARGE`; a live session is never ended for its size, and
  `<p>File` / `<p>FileAccess` still read one row. Through 0.5.1 a list read
  the whole drive. *Migration:* a host whose drives can exceed the default
  raises `EngineConfig::with_window_capacity(n)`.
- **`known_runner_types(&Query<'_, H>) -> async_graphql::Result<Vec<DriveRunnerType>>`**
  (was `(pool, principal) -> Result<_, DriveFault>`): the engine hands a
  resolver no raw pool, so the catalogue copy is read through the new
  `DriveRunnerTypes` view. *Migration:*
  `br_drive::known_runner_types::<P>(&service_engine::Query::<P>::new(ctx)?).await`.
- **`runner_context(&Query<'_, H>, principal, file_id, job_id) -> async_graphql::Result<RunnerContext>`**
  (was `(pool, principal, file_id, job_id) -> Result<_, DriveFault>`): read
  behind the runner source gate (`Query::behind`), in one read-only
  snapshot. *Migration:* pass `&Query::<P>::new(ctx)?` for the pool and drop
  the `.map_err(DriveFault::into_graphql)`.
- **`DriveReactionFault::Store(EngineError)`** (was `Store(String)`), built
  by `From<EngineError>` (`#[from]`) and kept as the source of the
  dead-letter and log text; a database fault a retry cannot fix is now
  `Store(..)` with the `Terminal` disposition (was `Terminal(text)`), and the
  type is no longer `UnwindSafe` / `RefUnwindSafe` (as the engine's own
  reference faults). *Migration:* match `Store(error)` on the `EngineError`,
  build it with `.into()` / `?`; `Terminal(code)` keeps a refusal's code.
- **A store or engine fault never reaches a client as text.**
  `DriveFault::into_graphql` answers the engine's coded `INTERNAL` for
  `DriveFault::Engine` (logged with its cause chain through `OrInternal`);
  through 0.5.1 it sent the error's `Display`, database text included.
  `DriveFault::Engine` keeps the `EngineError` as its `source()` and writes
  only its own context in `Display`. *Migration:* none, unless a client read
  that text: read the log instead.
- **The stores' one-key read is the engine's.** `FileProcessingStore`,
  `LabelStore`, `RulesetStore` and the stores behind `FileRow`,
  `ImageRecord` and the page and file views no longer carry a
  `Persistence::load` (the engine 0.4.0 trait has none). *Migration:*
  `use service_engine::PersistenceExt;` and call `RulesetStore::<P>::load(conn, &id)`
  (the same call as before, now from the extension trait).

### Added

- **`DriveRunnerTypes<H>`**, the closed view behind `<p>RunnerTypes`, over
  the read-only `RunnerTypeStore` (`save` and `create` refuse: the watch is
  the only writer) and the noun `KnownRunnerType`, open-access
  (`RunnerTypeAccess`) and gated on the host's `ReadRulesets`; `register`
  registers it (projector `drive_runner_types`). `DriveRunnerType` and
  `DriveRunnerTypeLifecycle` derive `Serialize` / `Deserialize`.
- `ImageStore` implements the batched `read_many` (one statement over
  `(file_id, name)` pairs).

### Changed

- `watch_runner_types_of(engine)` keeps its signature but runs on a small
  pool of its own (at most two connections, connected lazily) on
  `DATABASE_URL` — the URL `run_service` boots the engine's pool from — since
  the engine hands its pool to no caller; without a usable `DATABASE_URL` it
  logs a `warn` and does not start. A host holding its pool keeps calling
  `watch_runner_types(nats, pool)`.
- Every view's `populate` binds the engine's ceiling as the `LIMIT` of its
  key read (`cx.limit_all()`), so a refused attach or list reads at most one
  key past the capacity.
- The runner source presign is gated in `RunnerSources::visible` — a
  runner, inside a runner scope, the job's own file, with that job active for
  the presign (`scoped_to_job`); no scope admits no row — since
  `Query::download` now gates on the row's visibility rather than the view's
  window.
- The example host: a pure `from_passport`, its faults keep their source,
  its second test host loads its drives through a fact loader, and a
  scenario proves the window capacity on a drive (the list and a new session
  refused, a live session kept, one file read and downloaded).
- README: the engine compatibility table gains its 0.6 row (`v0.4.0`); the
  window capacity and the catalogue watch's own pool are documented.

## 0.5.1 — 2026-09-30

Additive only: no signature or behaviour of 0.5.0 changes, and a host on
0.5.0 upgrades by bumping the pin (one additive migration).

### Added

- **Choosing the upload rule at upload time**: `RequestUpload` takes an
  optional `rulesetId` — an `upload` rule matching the file's media type,
  validated as `ProcessFile` validates a named rule (`RULESET_NOT_FOUND`,
  `RULESET_MISMATCH`), pinned on the pending file
  (`drive.file.upload_ruleset_id`, migration `9121000012`) and recorded as
  `FileEvent::UploadRulesetChosen { ruleset_id }` right after
  `UploadTicketIssued`. The commit's chain (`process_on_commit`) runs it in
  place of the default; a chosen rule deleted or changed before the commit
  stores the file `READY`, never processed by the default in its place.
  Without `rulesetId`, the 0.5.0 gesture runs unchanged. In Rust, the named
  variant is its own input, `RequestUploadWithRuleset { upload, ruleset_id }`
  (mutation `drive_request_upload_with_ruleset`): `RequestUpload` keeps its
  fields.
- **`processing_counts(conn, &drive_ids)`** beside `file_counts`: each
  drive's files per processing state, `ProcessingCounts { pending,
  processing, ready, failed }` (`files()` their sum), in one statement — a
  host stops reading `drive.file_processing` for its processing and failed
  counts.
- **`freeze_drive(ops, &meta, drive_id) -> DriveFrozen { cancelled,
  abandoned }`**, in the host's own gesture and transaction: every running
  processing of the drive ends as a cancel — `job.cancel.v2` staged,
  `JobCancelSent` then the new `ProcessingEvent::JobCancelledOnFreeze`, the
  file `FAILED` `cancelled` at once, so the runner's next call is
  `JOB_NOT_ACTIVE` and Jobs' own `cancelled` is recorded as ignored — and
  every pending upload is abandoned as its deadline would (`UploadAbandoned`,
  blob released). No frozen flag is stored: the host's gate refuses new
  gestures, and a gesture crossing the freeze is the host's to refuse in
  `record_facts` (README, "Freezing a drive").
- **`delete_drive_in_reaction(cx, drive_id)`**: `delete_drive` from a host
  **reaction**, which the engine gives no bulk pipeline — the same effects
  (files, objects, running jobs cancelled, `DriveDeleted` facts under the
  reaction's message), impacts file by file up to `BULK_RESET_THRESHOLD`
  and none past it (a reaction can stage no projector reset).
- The example host: `workspaceFreeze`, a `retract` command whose reaction
  deletes a workspace and its drive, and the workspace view's
  `pendingFileCount`, `processingFileCount`, `failedFileCount`; e2e for each
  capability.

### Changed

- README: the engine compatibility table gains its 0.5 row (`v0.3.4`).

## 0.5.0 — 2026-09-30

State tables only, soft-EDA: the library ships **no fact table** any more.
Each host owns one fact table for everything it does, and the library hands
it its facts in the transaction that writes the state.

### Breaking

- **New required `DriveHost::record_facts(conn, &[DriveFact])`**: a host that
  does not implement it does not compile. Every change of a library state row
  reaches it, in the gesture's transaction; an `Err` rolls the gesture back.
  The README gives the reference fact table a host creates (the example host
  creates it as `workspace_fact`).
- **Dropped tables** (migration `9121000011`, rows included): `drive.fact`,
  the five job fact tables (`drive.file_job`, `drive.file_job_end`,
  `drive.file_job_cancel`, `drive.file_job_plan`, `drive.file_job_step`) and
  the `drive.file_status` view. A host reading them reads its own fact table
  and `drive.file_processing` instead.
- The stores of files, pages, labels, rules and file processing are
  `PersistenceStyle::SoftEda`: their `Persistence::Event` is
  `Stamped<…Event>`, and `LabelStore` / `RulesetStore` are generic over the
  host (`LabelStore<H>`, `RulesetStore<H>`), their aggregates `LabelRecord<H>`
  / `RulesetRecord<H>` wrapping the unchanged `LabelRow` / `RulesetRow`. The
  crate-private `fact` module is gone.

### Added

- `DriveFact`, `FactMeta` (aligned on `br-core-events`' `EventMetadata`:
  `actor_id`, `actor_kind` `human | service`, `is_runner`, `impersonator_id`,
  `correlation_id`, `causation_id` — the inbound message of a reaction —
  `occurred_at`) and `ActorKind`.
- One event enum per aggregate, tagged by `kind`, each at payload version 1:
  `FileEvent` (`drive_file`), `PageEvent` (`drive_page`, keyed
  `{file_id, number}`), `LabelEvent` (`drive_label`), `RulesetEvent`
  (`drive_ruleset`) and `ProcessingEvent` (`drive_file_processing`). `seq` is
  the aggregate's version after the event: gap-free per `(noun, key)`.
- **`drive.file_processing`**: one state row per processed file — its last
  job, the chain step, the initiator, the error, where the runner says it is,
  the cancel request, the earlier job ids, `version` and `updated_at`. No row:
  `PENDING` or `READY`, as before.
- **Every Jobs fact received is recorded**: the ones that change nothing any
  more — an end after the job's first end, a progress fact of an ended job
  or one already known, any fact about an earlier job of the file — as
  `JobFactIgnored { job_id, received, why }`; Jobs' `queued`, `started` and
  `completed` as themselves.
- `version bigint NOT NULL` on `drive.file`, `drive.file_page`,
  `drive.label`, `drive.ruleset` (and `drive.file_processing`).

### Changed

- The last change is state again: `drive.file.updated_at`,
  `drive.file_page.updated_at` / `updated_by`, `drive.label.updated_at`,
  `drive.ruleset.updated_at`, written by the store with every event. A file's
  `updatedAt` is the latest of its row's and its processing's. The GraphQL
  API and the live deltas are unchanged; a file's `updatedAt` now also moves
  on its label and image changes, which are facts of the file.
- The library's erase rewrites the chain initiator on `drive.file_processing`
  and a page's `updated_by`; the facts, in the host's table, are the host's to
  erase.
- Deleting a label loads the files it is on through the pipeline
  (`cx.load_many`) before the label, in the engine's lock order, and records
  `LabelsChanged` on each; `SetFileLabels` loads every label it touches after
  the file, so the two serialise. The erase locks the files whose chain names
  the person through the pipeline too. No raw row lock is left outside the
  stores' `Persistence::lock`.
- Facts state what happened, never a request: the file's creation is
  `UploadTicketIssued`, an image's `ImageUploadTicketIssued`, a user's cancel
  `JobCancelSent`. The wire causes keep their names (`UploadRequested`,
  `ImageRequested`, `CancelRequested`).
- A cancel crossing a step's final report records `StepSkipped { step_index,
  runner_type, after_job_id }`: no job id is minted for a step never
  launched, and the file's processing keeps the ended job as its last one
  (visible outcome unchanged: `FAILED` `cancelled`).

- **The commit starts the upload rule** (`DriveHost::process_on_commit`,
  default `true`): `CommitUpload` starts the chain of the default `upload`
  rule matching the file's media type in its own transaction, when the host's
  `Process` gate allows it — `PENDING` → `PROCESSING`, never `READY` first;
  `UploadCommitted`, `ChainStarted` and `JobCreated` share the commit's
  correlation, and a failure rolls the commit back whole. No matching rule:
  the file is `READY` (stored only), the commit is not refused. `ProcessFile`
  stays for reprocessing, and for a host that turns the switch off to keep
  the two-gesture flow. **Breaking** for a host that relied on a commit never
  processing: it overrides `process_on_commit` to `false`.

### Upgrading from 0.4

- Create the host's fact table and implement `record_facts` before booting
  0.5.0. Migration `9121000011` builds `drive.file_processing` from each
  file's last job and its end (the rules of the dropped view), backfills
  `version` to 1 and `updated_at` / `updated_by` from each object's last
  `drive.fact` row (else its creation; a page: its file's), then drops the
  fact tables, `drive.fact` included, with their rows. Every
  `processingState`, `processingError`, `progress` and `updatedAt` reads as
  before. A migrated aggregate's first fact in the host's table is `seq` 2.

## 0.4.0 — 2026-09-28

### Added

- **An audit trail of append-only facts** (`drive.fact`, migration
  `9121000010`), shaped like the identity service's: every change of a file
  (rename, move, folder move, retitle, metadata, commit,
  processing started, report stored, processing finished or failed, cancel
  requested), of a page (reported, edited), of a label (updated) or of a rule
  (saved) is a fact naming its `actor_id`, its `actor_kind` (`human`,
  `service`, `runner`), the admin behind an impersonated session
  (`impersonator_id`), the gesture's `correlation_id`, the change
  (`fact_type`, `payload`) and `occurred_at`. The engine keeps no such trail
  (its `event_log` serves event-sourced aggregates only and records no actor),
  so the library does, once.

### Changed

- The columns overwritten on every change are gone — `drive.file.updated_at`,
  `drive.file_page.updated_by` / `updated_at`, `drive.label.updated_at`,
  `drive.ruleset.updated_at`: the last change is read from the facts. The
  GraphQL fields stay, with the same shape: `DriveFile`, `DriveLabel` and
  `DriveRuleset`'s `updatedAt` are the object's latest fact (its creation when
  none); `DrivePage.updatedBy` / `updatedAt` and `RunnerPage.updatedAt` the
  page's latest fact. `FileRow`, `LabelRow`, `RulesetRow` and `PageRecord` keep
  their fields, read the same way.
- An erase anonymises the facts naming the person, as actor or as
  impersonator; the facts stay.

### Removed

- The `protected` mark — no host uses it: `br_drive::set_protected`, the
  `drive.file.protected` column (dropped by migration `9121000010`),
  `DriveFile.protected` and `FileRow.protected`, the `FILE_PROTECTED` code and
  every refusal of a rename, a move, a delete or a folder gesture on a
  protected file, the `FileCause::ProtectionChanged` cause (and fact), the
  example host's `workspaceProtectFile`. A host keeps such a rule in its own
  gate.

### Upgrading from 0.3

- A host that called `set_protected` or matched `FILE_PROTECTED` or
  `ProtectionChanged` moves that rule into its own gate (on the row's
  metadata, say, as the example host's `{"hold": true}`).
- Migration `9121000010` carries each object's last change over as one fact
  (`LastChangeCarriedOver`, actor kind `unknown`: 0.3 did not record who; a
  page keeps its recorded writer as the fact's actor), for every page and for
  every file, label and rule changed since its creation, then drops the
  overwritten columns and `drive.file.protected`. One-way.

## 0.3.0 — 2026-09-28

### Added

- The job config sent with `job.create` names the failure-declaration root,
  `failure_root` (`<p>RunnerReportFailure`), beside `context_root`,
  `image_upload_root` and `report_root`; `br_drive::RootNames` gains
  `failure_root`. A runner declares its failure on the root its config
  names.

### Removed

- The host-privileged import: `<p>ImportPages` and `<p>ImportImage` (and
  `br_drive::ImportPages`, `ImportImage`, `ImportedPage`,
  `ImportedPageInput`), `DriveRequest::Import { file }`,
  `FileRow::import_gate`, `DriveHost::IMPORT_SCOPE` and
  `DriveHost::is_importer`, the `IMPORT_SCOPE_REQUIRED` code, the
  `FileCause::RenditionImported` and `PageCause::Imported { origin }` causes.
  A host that moves an existing corpus in writes the `drive` schema itself;
  the README lists the invariants such rows must hold ("Data migrated
  directly into the schema").
- The example host's migration account (`workspace:import`, its upload,
  commit and import rules, `NOT_AN_IMPORTER`): a commit is the uploader's
  only.

### Upgrading from 0.2

- Drop `IMPORT_SCOPE` from the `DriveHost` impl, every `is_importer()` call
  and the `DriveRequest::Import` arm of `drive_gate`; a gate that let an
  import account through `CommitUpload { file }` loses that rule. A client of
  `ImportPages` / `ImportImage` moves to a migration writing the schema
  directly. No schema change: the migrations stay 01 to 09.

## 0.2.1 — 2026-09-28

### Changed

- **The job log is typed, insert-only fact tables** (migration `9121000009`),
  the shape Jobs gives its own ledger; nothing is stored that is a state or
  derives one. 0.2.0's `drive.file_job.seq` (a global order), `events` (a
  jsonb array rewritten on every fact) and `outcome` (a stored generated
  column) are gone, and so is the partial "one live job" index:
  - `drive.file_job` — the job was created: `file_id`, `number` (1, 2, 3… per
    file, assigned max + 1 under the file's row lock), `job_id` (unique),
    `step_index`, `trigger`, `triggered_by_id` / `triggered_by_name`,
    `created_at`; primary key `(file_id, number)`. The file row lock every
    writer takes first, and the fact that a new job is only created once the
    last one ended, keep one running job per file; the primary key refuses a
    second job numbered alike should a writer skip the lock.
  - `drive.file_job_end` — the job ended: `kind` (`reported_done`,
    `reported_failed`, `failed`, `cancelled`, `creation_rejected`),
    `reason_code` (required for the three failures, absent otherwise),
    `message`, `at`; primary key `job_id`, so the first end wins by
    construction (`ON CONFLICT DO NOTHING`) and a later one is not stored.
  - `drive.file_job_cancel` — a user asked to cancel: `requested_by`
    (anonymised by an erase), `at`; primary key `(job_id, number)`.
  - `drive.file_job_plan` — the runner declared a plan (`run_id`, `labels`,
    `declared_at`; the highest `number` is current; a redelivered declaration
    of the current plan adds nothing) and `drive.file_job_step` — it started a
    step of its plan (`run_id`, `plan_index`, `label`, `started_at`; primary
    key `(job_id, run_id, plan_index)`, so a redelivery adds nothing and a
    retry run starts its steps again).
  - Jobs' `queued`, `started` and `completed` are no longer stored: nothing
    read them. Their reactions stay bound and acknowledge them, so a host's
    durables never hold them. Progress facts of a job that no longer runs are
    not stored either.
- The `drive.file_status` view reads the file's highest-numbered job and its
  end (no end → `PROCESSING`; `reported_done` → `READY`; any other end →
  `FAILED` with its reason), and, while the job runs, its current plan, its
  latest step and whether a cancel was asked. Statuses, errors, progress,
  gates and affordances are unchanged; so are the Rust API and the SDL.
  `FileJob::events` now holds what the running job's reads need, built from
  the fact tables when the job is read (the current plan, the latest step, a
  cancel request), and is empty once the job ended.

### Upgrading from 0.2.0

- Migration `9121000009` explodes each 0.2.0 job row: its `number` follows
  its file's former `seq` order; the first terminal entry of its `events`
  becomes its `drive.file_job_end` row (a failure's reason: the stored
  `reason`, else the failure report's `reason_code`, else Jobs'
  `failure_cause`; a runner's declared message or Jobs' note as `message`);
  every `cancel_requested` becomes a `drive.file_job_cancel` row (without a
  requester: 0.2.0 did not record one); the `plan_declared` and
  `step_started` entries become plan and step rows. Then `seq`, `events`,
  `outcome`, `triggered_by` (now `triggered_by_id` / `triggered_by_name`) and
  the `file_job_one_live_idx` / `file_job_file_idx` indexes go. One-way.

## 0.2.0 — 2026-09-28

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
  against the file and host the rejection names) is cancelled at once, and
  the user's reprocess gets through (a cancel is idempotent).
- `job.create` carries only what Jobs accepts: `RequestUpload` refuses a file
  id that is not a UUIDv7 (`INVALID_FILE_ID`) — Jobs refuses a non-v7 source
  entity, which would fail every chain of the file — and `triggered_by` is
  trimmed, a blank display name sent as anonymous, a long one cut at 512
  characters, an erased (nil) initiator not named at all.
- The runner source presign is scoped to the one file the job names, while
  that job is the file's active one, instead of enumerating every file with a
  live job on each `RunnerContext` call; the population re-checks the active
  job with the same rule as the runner roots.
- Image names are no longer capped at page 999 and image 99: the page and
  index widths of `p{page:03}-img{n:02}.{ext}` are minimums (`p1000-img100.png`
  is accepted), a number is never padded wider than it needs, an index is
  never zero.

- The host's gate is asked before the file's state, and a refusal about a
  file the principal cannot see answers `FILE_NOT_FOUND`, as an unknown id
  does: `process`, `editPage`, `regeneratePage`, `download`, the protected
  refusals, the commit, `setLabels` and the metadata write disclosed the
  existence (the host's code) or the state (`FILE_PROCESSING` /
  `FILE_NOT_READY` / `FILE_NOT_PENDING` / `FILE_PROTECTED`) of a file the
  principal may not see. A move toward an unknown drive answers the host's
  refusal before `DRIVE_NOT_FOUND`, so a drive id is no oracle either.
- `CommitUpload` asks the host a request of its own, `CommitUpload { file }`,
  carrying the pending row and its `created_by`, instead of re-asking
  `CreateFile` without the row: in a shared drive any principal allowed to
  create files passed the commit gate for somebody else's pending upload.
  `FileRow::as_create_request` and `FileRow::require_pending` are gone;
  `FileRow::commit_gate` replaces them, and the decision is projected as the
  `commit` affordance.
- The label catalogue is gated: `<p>Labels` and `<p>LabelsChanged` consult
  the new `DriveRequest::ReadLabels`, mirroring `ReadRulesets`, instead of
  serving every principal of the host, the runner included.
- `br_drive::set_metadata` asks the host's gate
  (`DriveRequest::SetMetadata { file }`) for the principal it now takes —
  pass the mutation's or reaction's own principal:
  `set_metadata(ops, principal, file_id, metadata)`; the decision is
  projected as the `setMetadata` affordance.
- The label and ruleset live windows follow the principal's facts: a
  principal who gains or loses `ReadLabels` / `ReadRulesets` sees the window
  repopulate at once instead of at the next catalogue change.
- `MoveFolder` and `DeleteFolder` ask, after the folder-level gate, the
  per-file decision of every file under the prefix — the `UpdateFile` /
  `DeleteFile` request the per-file gesture asks, `FILE_PROTECTED` included —
  all or nothing: in a drive shared by several people a principal could move
  or delete other people's files through a folder although the host refused
  them the same gesture on each file. The first refusal in path order answers
  (the host's code or `FILE_PROTECTED`); a file the principal cannot see
  answers `FOLDER_NOT_FOUND`, as an empty prefix does. The decisions run in
  memory over the rows the bulk path already loads, past the reset threshold
  too.
- A file whose job no runner picks up (a runner type Jobs never declared, no
  live instance) can be cancelled by its user (`<p>CancelProcessing`, see
  Added): Jobs' `cancelled` lands it `FAILED`, and a reprocess starts over. A
  job Jobs no longer holds live is not rescued by it (see the README's
  Follow-ups).

### Changed

- **A file's processing state is computed, never stored.** Every Jobs job the
  library creates for a file (one per chain step) is a row of the new
  `drive.file_job` log (migration `9121000007`): the job, its step, the
  gesture that started its chain (`trigger`), its initiator, and the
  append-only list of what is known of it, each `{kind, at, ...payload}` —
  every Jobs fact received (`queued`, `creation_rejected`, `started`,
  `plan_declared`, `step_started`, `completed`, `failed`, `cancelled`) and what
  the library writes itself (`reported_done`, `reported_failed`,
  `cancel_requested`). The `drive.file_status` view computes
  `PENDING | PROCESSING | READY | FAILED` (the shared vocabulary is unchanged)
  and the error from the file's last job and the new
  `drive.file.committed_at`: no job and not committed → `PENDING`; no job and
  committed → `READY` (stored, unprocessed); a last job whose first terminal
  entry is `reported_done` → `READY`; `reported_failed`, `failed`,
  `cancelled` or `creation_rejected` → `FAILED` with the error read from that
  entry; otherwise `PROCESSING`. `processingState`, `processingError` and
  `progress` are read from it (the SDL is unchanged), and so are every gate,
  affordance and `file_counts`. A fact updates only its own job, so a late
  fact of an older job changes nothing; a job settles on its first terminal
  entry, so anything arriving after it (the facts ride separate durables) is
  logged and changes nothing. A partial unique index keeps at most one
  unsettled job per file; "last job" follows an identity sequence, never the
  pods' clocks. An erase locks the files whose jobs name the person before
  rewriting the jobs' initiators.
- **The runner declares the end of its job; the library tells Jobs.** The
  runner's final report (`done: true`) ends the job in its own transaction:
  its results are stored, `reported_done` is logged, the chain moves on (the
  next step's job is created in the same transaction, or the chain is over
  and the file `READY`) and `job.finish.v2` is staged. Jobs' `completed` is
  logged as information and changes nothing. A declared failure (the new
  `<p>RunnerReportFailure`, see Added) logs `reported_failed` and stages
  `job.fail.v2`. Jobs' own `failed`, `cancelled` and `creation_rejected` stay
  terminal, the safety net for a runner that crashed or a user's cancel. A
  job ends once: a replayed final report meets a job that no longer runs
  (`JOB_NOT_ACTIVE`, nothing changes) — a runner retrying its final report
  after a lost ack takes `JOB_NOT_ACTIVE` as success.
- **Committing and processing are two gestures.** `CommitUpload` only
  confirms the upload (the storage check, `committed_at`): the file is
  `READY`, stored and unprocessed, whatever rule matches. `<p>ProcessFile`
  (see Added) is the only way to start a chain.
- **Reprocessing rewrites in place.** Nothing is wiped when a chain starts:
  the previous pages, images and indexing stay readable while the new run
  reports over them. A reported page replaces the page of the same number,
  except that an upload or reprocess run keeps a page whose origin is
  `EDITED`; a `regenerate_page` run overwrites it. Images are replaced by
  name as reported; the indexing when a report carries one. When the chain's
  last step lands its final report, the pages numbered above the file's page
  count are deleted — edited ones included — and the images no page's
  markdown references any more are dropped and their objects released
  (`ImagesDropped`). A failed reprocess leaves the file `FAILED` with the
  previous results still readable.
- **A file's results have their own table.** `summary`, `page_count` and
  `estimated_tokens` move from `drive.file` to the new
  `drive.file_processed` (migration `9121000008`), which holds only what the
  workers produce; `drive.file_page` and `drive.file_image` hang off it
  (cascade). The row is created by the first write that needs it (a report,
  an image request, an import). `DriveFile` still exposes `summary`,
  `pageCount` and `estimatedTokens`; `drive.file` keeps the plan
  (`ruleset_id`, `steps`) and `committed_at`.
- `updatedAt` moves on every write that changes the file's status: a Jobs
  fact that settles the running job, a cancel request, the runner's final
  report or declared failure. Progress facts (`ProgressChanged`) no longer
  touch the host object — they never change a count — and `file_counts`
  reads the status view by drive (the view carries `drive_id`), served by
  `file_drive_idx` and the job log's index.
- `FileRow`'s `processing_state` and `processing_error` fields became
  computed accessors, `processing_state()` / `processing_error()`, beside
  `last_job()` / `active_job()` (`br_drive::FileJob`) and `committed_at`; the
  step, job, plan, progress, initiator and clock fields are gone.
- The runner-type catalogue copy is information only. `drive.known_runner_type`
  and the optional `br_drive::watch_runner_types` / `CatalogueWatch` (the
  example's `BootOptions::watch_catalogue`) stay; the copy is read by
  `RulesetSaved.unknownRunnerTypes` (and `RulesetCause::Saved {
  unknown_runner_types }`) — the steps' types not known `ACTIVE`, a warning on
  a rule that is saved — and by the new `<p>RunnerTypes` (Added). A rule may
  name any runner type; a step's job is created whatever the copy says, and
  Jobs alone judges it. On a host that does not start the watch every step's
  type is reported unknown and no type is listed; nothing else changes. Only
  such a host's broker may lack the `PUBLISHED_LANGUAGE` bucket.
- The catalogue watch opens its KV watch before its scan and drains it after,
  so a put or a delete landing during the scan is no longer lost. A name over
  the copy's 128 bytes is left out instead of faulting the watch, a value that
  is not JSON at all only takes its type out once the watch follows the
  bucket (the start-up scan still stops on one: see the README), and a fault
  is retried after a pause doubling from 1 s to 1 min instead of every second.
  `CatalogueWatch` is `#[must_use]` and gains `detach`; the new
  `br_drive::watch_runner_types_of(&engine)` starts the watch on the engine's
  own handles from a `BootPlan` host's `register` closure.
- The token estimate of a runner report is optional: `summary` and
  `pageCount` still move together, `estimatedTokens` may be absent (it may not
  come alone). An indexing without an estimate clears a previous one.
- `FileCause` is `#[non_exhaustive]`; new variant `CancelRequested { job_id }`.
- `FileJob` carries its chain's `trigger`; `DriveRequest::Process { file }` is
  now asked for every processing, the first one included.
- `br_drive::create_drive::<H>(ops, owner, created_by)` takes the host object
  the drive hangs off (`&<H::DriveOwner as DriveOwnerNoun>::Object`) and gives
  the drive its key, instead of a free id: a drive's id is its host object's
  id by construction, which the host refresh (`DriveOwner`) relies on.
  `DriveOwnerNoun` gains `type Object` (a host aggregate keyed by a UUID);
  `NoDriveOwner`'s is `Unowned`, which has no value.
- A chain step never names a `parent_job_id` (see Fixed).
- Image names accept wider page and index numbers (see Fixed).
- Engine pin `v0.3.0` → `v0.3.4`: authenticated bodies are bounded (0.3.3),
  and subscriptions are served over graphql-sse on `POST /graphql`, the
  transport the gateway uses (0.3.4). The e2e harness's owner role is
  `BYPASSRLS`, as `migrate` now asserts (0.3.1).

### Removed

- `<p>Process` (folded into `<p>ProcessFile`) and `CommitUpload`'s
  `rulesetId`: a commit never processes.
- The wipe of the pages, images and indexing at the start of a reprocess
  (`br_drive::wipe_rendition`), and `br_drive::finish_job` (the chain stages
  `job.finish` itself).
- Every use of the runner-type catalogue copy as a condition:
  `drive.catalogue_scan` (the "catalogue was read" record), the deferred
  launch (`launch-retry`, `LAUNCH_RETRY_AFTER`, `LAUNCH_RETRY_CAP`,
  `FileCause::LaunchDeferred`), the `runner_type_unavailable` failure, the
  `RUNNER_TYPE_UNAVAILABLE` and `CATALOGUE_NOT_WATCHED` codes. Jobs refuses at
  creation a runner type it retired (`creation_rejected`
  `runner_type_retired`: the file fails), and accepts one it does not know
  without ever dispatching it: the user's cancel is the way out.
- Both processing deadlines: `DriveHost::PICKUP_TIMEOUT`,
  `DriveHost::STEP_TIMEOUT`, the `step-deadline` message and its durable, the
  `timed_out` failure. The upload deadline stays, keyed on `committed_at`.
- The stray job kept on a file after a `duplicate_active_entity` rejection,
  and the automatic relaunch when a cancel and a create crossed.
- The migrations `main` carried unreleased since 0.1.0
  (`processing_backstops`, `file_counts_index`, `run_started_at`): the chain
  goes from 0.1's schema to the job log directly; the title migration is now
  `9121000006`.

### Added

- `<p>ProcessFile(fileId, rulesetId?)`: the user starts processing a stored
  file — right after its commit, later, or never — through the host's
  `DriveRequest::Process { file }` gate (affordance `process`). Allowed on a
  `READY` or `FAILED` file (`FILE_NOT_READY` on a pending upload,
  `FILE_PROCESSING` while a chain runs). A file that never had a job runs
  its `upload` rule, any other its `reprocess` rule: the given rule (it must
  carry that trigger, `RULESET_MISMATCH` otherwise), else the default of that
  trigger for the media type, else — for a reprocess — the file's own
  snapshot, else `NO_RULESET_MATCHES`. Exposed in the example host.
- `<p>RunnerReportFailure(fileId, jobId, reasonCode, message?)`: the runner
  declares its job failed, with the same checks as a report (the runner
  scope, the file's running job). `reasonCode` is a code, `[a-z][a-z0-9_]*`
  up to 128 bytes, `message` up to 4 KiB (`INVALID_FAILURE_REASON`
  otherwise). The file lands `FAILED` with that code as `processingError`
  (`ProcessingFailed { reason }`), `job.fail.v2` is staged with the code and
  the message as its note, and the results reported so far stay.
- `<p>RunnerTypes: [DriveRunnerType!]!` (`br_drive::known_runner_types`): the
  whole local catalogue copy — `{ runnerType, lifecycle: ACTIVE | DEPRECATED,
  seenAt }` in name order — for a host's rule-editing screen. Gated by the
  host's existing `ReadRulesets` (a refused principal gets an empty list, as
  for `<p>Rulesets`); a plain read, no live window.
- `<p>CancelProcessing(fileId)`: the user cancels the job running on a
  `PROCESSING` file, through the new `DriveRequest::CancelProcessing { file }`
  gate (affordance `cancelProcessing`; `FILE_NOT_PROCESSING` otherwise). The
  request is logged on the job (`cancel_requested`, cause
  `CancelRequested { job_id }`) and `job.cancel.v2` is staged; the file stays
  `PROCESSING` until Jobs' `cancelled` lands it `FAILED` `cancelled`, open to
  a reprocess. It may be asked again (a cancel Jobs consumed before the job's
  creation is dropped). A cancel that crosses a step's final report stops the
  chain there: the next step is recorded as never started (`cancelled`, a
  row of the library's own, never sent to Jobs) and the file is `FAILED`
  `cancelled`; one that crosses the last step's final report has nothing
  left to stop — the file is `READY`. The example host exposes it to a
  workspace's owner.
- `br_drive::create_unowned_drive::<H>(ops, id, created_by)`, for a host whose
  `DriveOwner` is `NoDriveOwner` only.
- A host-privileged **import** of an existing rendition:
  `<p>ImportPages(fileId, pages, summary?, pageCount?, estimatedTokens?)` and
  `<p>ImportImage(fileId, name, mediaType, size, sha256)`, gated by the new
  `DriveRequest::Import { file }` behind the library's own `IMPORT_SCOPE`
  opt-in, and allowed on a `READY` file only, with no job and no runner
  scope. Pages keep their origin (`RUNNER` or `EDITED`; `REGENERATED` is
  refused) and go
  through the same upsert and the same impacts as a runner report
  (`Imported { origin }` on the pages, `RenditionImported` on the file); the
  image reuses the verified runner image path. The runner's image staging
  and rendition validation are shared with it. The migration commits its
  uploads with the ordinary `<p>CommitUpload` — a commit never processes, so
  no commit of its own is needed: the host's `CommitUpload { file }` gate lets
  its importer (`DriveHost::is_importer`) through for the uploads it made, as
  the example host does.
- `DriveHost::DriveOwner`: a host names its own noun type, keyed by the
  drive id (`impl br_drive::DriveOwnerNoun for Its {}`), or
  `br_drive::NoDriveOwner`; every file change the library stages also
  impacts that key, without a cause, so the host's views bound to its own
  noun — an object carrying file counts — recompute and republish while files
  land, fail, move and go. No host callback; the noun and its key are
  compiler-checked. `br_drive::file_counts` reads the file and READY counts
  of several drives in one statement, from the computed status. The example host's `WorkspaceView` gains `fileCount` and
  `readyFileCount`, live.
- `DriveHost::IMPORT_SCOPE` (default `None`: no import): the scope a service
  account must hold to import, checked by the library before the host's gate
  (`IMPORT_SCOPE_REQUIRED`), the way the runner scope is.
- A file's **title**: `drive.file.title` (migration `9121000006`, at most 255
  characters, existing files backfilled with their name without its
  extension), projected as `DriveFile.title`. `RequestUpload` takes an
  optional `title` (the `FileTitle` value object, built only by parsing:
  trimmed, 1–255 characters, one line, no control or bidirectional-override
  character, `INVALID_TITLE`); absent, it is the requested name without its
  extension. `<p>RetitleFile(fileId, title)` changes it through
  the new `DriveRequest::RetitleFile { file }` gate (affordance `retitle`,
  cause `Retitled`); renaming or moving never touches the title, retitling
  never moves the file.
- The example host's Jobs stand-in judges every `job.create` the way Jobs
  does, in Jobs' order — the inputs `svc-jobs` refuses before the domain
  (non-v7 ids, a blank or over-long display name, a source not named by its
  producer), a runner type it retired, a reused id, a second live job on a
  source entity, a self-named, terminal, deleted or unknown parent — and
  answers `creation_rejected` itself; it publishes `cancelled` for a live job
  it cancels (or drops every cancel on demand), and a create a scenario
  awaits fails the scenario when refused. Regression scenarios for the
  double; scenarios for the duplicate-job trap, the user's cancel and late
  facts. The example host's gate reserves a commit to the uploader, keeps
  service principals out of the label catalogue, and its metadata mutation
  relies on the library's gate.

### Upgrading from 0.1

- Pin `br-service-engine` `v0.3.4`, the same tag as `br-drive`. The
  owner role `migrate` runs as must be `BYPASSRLS` (engine 0.3.1 refuses
  otherwise with `OwnerSubjectToRls`). Bodies are bounded at 16 MiB by
  default (engine 0.3.3): a host that imports or accepts runner reports
  larger than that raises `MultipartConfig::max_body_bytes`.
- `DriveRequest` gains `CommitUpload { file }`, `ReadLabels` and
  `SetMetadata { file }`: decide each explicitly — a wildcard arm in
  `drive_gate` now answers them silently. A 0.1 host whose `CreateFile` rule
  checked the path, name, media type or size relied on it being re-asked at
  commit: re-apply it on `CommitUpload { file }` (`file.path`, `file.name`,
  `file.media_type`, `file.size_bytes`). `ReadLabels` was implicitly allowed
  to every principal; `SetMetadata` was not asked at all.
- A host that mapped refusals to its own "not found" can drop that: the
  library answers `FILE_NOT_FOUND` for a file outside `visible_drives`.
- `DriveRequest::RetitleFile { file }` is new: decide it explicitly (a
  wildcard arm answers it), including for a `protected` file — the library
  does not refuse a retitle on protection. `RequestUpload` takes an optional
  `title`.
- Migration `9121000006` is one-way: once applied, a pre-0.2 binary cannot
  create a file (`title` is `NOT NULL`).
- Migration `9121000007` replaces the stored processing state with the job
  log, one-way: `committed_at` is backfilled from `updated_at` for every file
  past `PENDING`. A job in flight becomes its file's log: with no final report
  yet, an empty log (the file stays `PROCESSING`, and the runner's next report
  or Jobs' next fact lands in it); with the final report received
  (`done_at`), `reported_done` — the end of the chain on its last step
  (`READY`), else the chain is recorded as interrupted before the next step,
  which 0.1 had not asked of Jobs yet (`FAILED` `interrupted`, open to a
  reprocess); with only Jobs' completion (`completed_at`, unreachable with the
  real Jobs), `completed` then `failed` `interrupted`. A `FAILED` file keeps its
  error as a synthetic `failed` entry of a synthetic job (never sent to
  Jobs); a `PROCESSING` file without a job, should one exist, lands `FAILED`
  `interrupted`. Then the stored state, step, job, plan, progress, initiator
  and clock columns and `drive.catalogue_scan` are dropped;
  `drive.known_runner_type` is kept as it is.
- Migration `9121000008` moves each file's `summary`, `page_count` and
  `estimated_tokens` to `drive.file_processed` for every file holding any
  result (an indexing, a page or an image) and repoints the foreign keys of
  `drive.file_page` and `drive.file_image` to it. One-way.
- A database that ran migrations `main` carried unreleased since 0.1.0
  (`9121000006`–`9121000009` there) must be recreated; a 0.1.0 database
  upgrades in place.
- `CommitUpload` no longer processes and takes no `rulesetId`: a front that
  wants a file processed calls `<p>ProcessFile` after the commit (the
  `process` affordance says when it may). `<p>Process` is now
  `<p>ProcessFile`; `DriveRequest::Process { file }` is now also asked for a
  file's first processing. A runner may declare its failure through
  `<p>RunnerReportFailure` instead of failing its run with Jobs; a runner
  that retries a final report after a lost ack gets `JOB_NOT_ACTIVE` once the
  first one landed, and must take it as success.
- `DriveRequest::CancelProcessing { file }` is new: decide it explicitly (a
  wildcard arm answers it). A host that started `br_drive::watch_runner_types`
  keeps it (optional, information only); one that set `PICKUP_TIMEOUT` / `STEP_TIMEOUT` drops them.
  A host that read `file.processing_state` / `file.processing_error` calls
  the accessors. A host that matched `RUNNER_TYPE_UNAVAILABLE`,
  `runner_type_unavailable` or `timed_out` may meet Jobs' own
  `runner_type_retired`, `cancelled`, a runner's declared code, or
  `interrupted`.
- `DriveHost` gains a required associated type, `DriveOwner`: write
  `type DriveOwner = br_drive::NoDriveOwner;` to keep 0.1's behaviour, and
  replace `create_drive(cx, id, created_by)` with
  `create_unowned_drive::<H>(cx, id, created_by)`. A host that names its own
  noun writes `impl DriveOwnerNoun for Its { type Object = ItsRow; }` and
  creates each drive from that row: `create_drive::<H>(cx, &row, created_by)`.
- `MoveFolder` / `DeleteFolder` now also ask the `UpdateFile` / `DeleteFile`
  decision of each file under the prefix: a host whose per-file rule is
  stricter than its folder rule sees folder gestures refused with that rule's
  code where they went through before.
- A migration commits its uploads with the ordinary `CommitUpload`: let the
  migration account through `CommitUpload { file }` (`is_importer()`, and
  `file.created_by` for the uploads it made).
  `DriveRequest::Import { file }` is new; an import additionally needs
  `IMPORT_SCOPE`, so a host that sets none offers no import whatever its
  gate answers.

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
