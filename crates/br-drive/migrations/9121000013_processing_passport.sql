-- The Passport of the principal whose gesture started the running chain
-- (upload commit, ProcessFile, RegeneratePage), as that principal presented
-- it: every step's `job.create` carries it, so the runner acts as that person.
-- Written in the transaction that starts the chain, set back to NULL in the
-- transaction of every end of the processing (READY, FAILED, cancelled, a
-- drive freeze); the row goes with the file. It exists here only while the
-- file is PROCESSING, and no fact, view or published event of the library
-- holds it. Its other copy on this side is the engine's outbox row of each
-- step's `job.create.v3` (`service_engine.integration_outbox`), swept after
-- the engine's message retention (a published row ~1 h after publication,
-- a failed one after its creation, a pending one only once published) and
-- not reached by the erase; Jobs keeps its own copy with the job.
-- A chain already running when this migration lands has none: its next step
-- ends FAILED `passport_missing` instead of being sent to Jobs without one.
-- The check is added NOT VALID: the column is new and NULL on every row, so
-- validating it could find nothing, and skipping the scan keeps the ACCESS
-- EXCLUSIVE lock short. It holds for every write from here on all the same.
ALTER TABLE drive.file_processing
    ADD COLUMN passport jsonb,
    ADD CONSTRAINT file_processing_passport_only_while_processing
        CHECK (passport IS NULL OR state = 'processing') NOT VALID;
