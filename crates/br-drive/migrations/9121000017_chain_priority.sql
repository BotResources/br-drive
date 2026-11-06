-- The priority a chain asks Jobs for (0.9.0): 1 (lowest) to 5 (highest),
-- named by the gesture that starts the chain. NULL asks for nothing, and
-- Jobs' own default applies. The library never judges it against the
-- initiator's ceiling: Jobs caps it at the maximum of the user the job runs
-- for.
--   drive.file.upload_priority  the priority the uploader pinned at
--                               `RequestUpload` for the commit's chain, like
--                               `upload_ruleset_id`; NULL when none was named
--   drive.file_processing.priority
--                               the running chain's, written in the
--                               transaction that starts it and asked by every
--                               step's `job.create`; set back to NULL at every
--                               end of the processing, like the Passport, so
--                               it is set only while `processing`
-- Additive: both columns are NULL on every existing row, so the checks are
-- added NOT VALID (nothing to scan, the ACCESS EXCLUSIVE lock stays short);
-- they hold for every write from here on. A 0.8.x binary ignores the columns
-- and asks Jobs for no priority, but never clears `priority` either: its end
-- of a chain a 0.9.0 binary started with one breaks the check. So upgrade
-- with no mixed 0.8.x / 0.9.0 consumers (a Recreate rollout, or one replica
-- during the upgrade), and let such chains drain (or set their `priority`
-- to NULL) before going back.
ALTER TABLE drive.file
    ADD COLUMN upload_priority smallint,
    ADD CONSTRAINT file_upload_priority_range
        CHECK (upload_priority BETWEEN 1 AND 5) NOT VALID;
ALTER TABLE drive.file_processing
    ADD COLUMN priority smallint,
    ADD CONSTRAINT file_processing_priority_range
        CHECK (priority BETWEEN 1 AND 5) NOT VALID,
    ADD CONSTRAINT file_processing_priority_only_while_processing
        CHECK (priority IS NULL OR state = 'processing') NOT VALID;
