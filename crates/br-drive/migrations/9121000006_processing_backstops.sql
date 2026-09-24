-- The processing backstops of 0.2: when the running step was entered (its
-- identity, on which the deadline and the deferred launch key), when it last
-- showed a sign of life (the deadline measures inactivity from there), and the
-- id of a job Jobs still holds on the file while the library had forgotten it
-- (a creation rejected as `duplicate_active_entity`, or a job the deadline
-- cancelled), kept until a new job of the file is queued so every launch can
-- cancel it first.
ALTER TABLE drive.file
    ADD COLUMN step_entered_at timestamptz,
    ADD COLUMN step_alive_at   timestamptz,
    ADD COLUMN stray_job_id    uuid,
    ADD CONSTRAINT file_step_clock_only_while_processing
        CHECK ((step_entered_at IS NULL AND step_alive_at IS NULL)
               OR processing_state = 'processing');
