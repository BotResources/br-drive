-- The Passport of the principal whose gesture started the running chain
-- (upload commit, ProcessFile, RegeneratePage), as that principal presented
-- it: every step's `job.create` carries it, so the runner acts as that person.
-- Written in the transaction that starts the chain, set back to NULL in the
-- transaction of every end of the processing (READY, FAILED, cancelled, a
-- drive freeze); the row goes with the file. It exists only while the file is
-- PROCESSING, and nothing else ever reads it: no fact, no view, no event.
-- A chain already running when this migration lands has none: its next step
-- ends FAILED `passport_missing` instead of being sent to Jobs without one.
ALTER TABLE drive.file_processing
    ADD COLUMN passport jsonb,
    ADD CONSTRAINT file_processing_passport_only_while_processing
        CHECK (passport IS NULL OR state = 'processing');
