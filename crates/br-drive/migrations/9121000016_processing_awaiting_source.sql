-- The first step of a chain, held until the engine promotes the file's
-- source (0.8.2). The engine serves no download URL for a verified source
-- until its blob reaper has verified the landed object and promoted the row
-- (`pending` -> `uploaded`); a job created before that would meet a runner
-- context with no source to read (`SOURCE_NOT_AVAILABLE`). So a chain started
-- on a source not yet promoted (the commit, or a `ProcessFile` right after
-- it) records its plan and mints its first job's id (`job_id`, step 0), the
-- file `processing`, and stages the `job.create` only when the promotion
-- reaches the library (the source's post-upload policy).
--   awaiting_source  the file's last job (`job_id`) is the chain's first step,
--                    decided and minted but not yet asked of Jobs: it waits
--                    for the source's promotion. Only while `processing`.
-- Additive: every existing row reads false. A 0.8.1 binary ignores the column
-- and never creates an awaited step: upgrade with no mixed 0.8.1 / 0.8.2
-- consumers (a Recreate rollout, or one replica during the upgrade), and let
-- awaited steps drain (none waits longer than one reaper interval) before
-- going back.
ALTER TABLE drive.file_processing
    ADD COLUMN awaiting_source boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT file_processing_awaits_only_while_processing
        CHECK (NOT awaiting_source OR state = 'processing');
