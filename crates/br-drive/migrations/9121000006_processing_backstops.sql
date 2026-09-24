-- The processing backstops of 0.2: when the running step was entered (the step
-- deadline and the deferred launch key on it), and the id of a job Jobs still
-- holds on the file while the library had forgotten it (a creation rejected as
-- `duplicate_active_entity`), kept so the next launch can cancel it.
ALTER TABLE drive.file
    ADD COLUMN step_entered_at timestamptz,
    ADD COLUMN stray_job_id    uuid,
    ADD CONSTRAINT file_step_entered_only_while_processing
        CHECK (step_entered_at IS NULL OR processing_state = 'processing');
