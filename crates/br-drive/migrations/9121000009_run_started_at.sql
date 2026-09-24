-- The processing deadline of 0.2 has two stages: a pickup deadline from the
-- step's job creation until Jobs reports the run started, then the run-silence
-- deadline. `run_started_at` records when the step's current job first showed
-- a started run (a `started` fact, a plan, a step or a runner report), which is
-- the instant the second stage takes over. Only while PROCESSING, like the
-- rest of the step's clock.
ALTER TABLE drive.file
    ADD COLUMN run_started_at timestamptz,
    DROP CONSTRAINT file_step_clock_only_while_processing,
    ADD CONSTRAINT file_step_clock_only_while_processing
        CHECK ((step_entered_at IS NULL AND step_alive_at IS NULL AND run_started_at IS NULL)
               OR processing_state = 'processing');
