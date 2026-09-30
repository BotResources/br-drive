-- State tables only: the library becomes soft-EDA. Every aggregate row
-- carries its `version` (one per event), its facts go to the host's own fact
-- table through `DriveHost::record_facts`, in the transaction that writes the
-- state. The library's fact tables go: the five job fact tables and their
-- `drive.file_status` view give way to one state row per processed file, and
-- `drive.fact` is dropped with its rows (no host ran 0.4.0 in production).
-- The last change of an object (`updated_at`, a page's `updated_by`) is a
-- state column again, written by the store on every change.

-- A file's processing: the state of its last job and of the chain it runs.
-- No row: PENDING (upload not confirmed) or READY (stored, never processed).
--   state                processing | ready | failed
--   job_id               the file's last job (the running one while processing)
--   step_index           its chain step, in the file's `steps`
--   trigger, triggered_* the gesture that started the chain and its initiator
--   error_code/_message  why a failed file failed
--   plan, plan_*         where the running job's runner says it is: its plan,
--                        and the latest step it started
--   cancel_requested_at  a user asked to cancel the running job
--   past_job_ids         the file's earlier jobs: a late Jobs fact about one
--                        is still recorded (ignored), never dropped
CREATE TABLE drive.file_processing (
    file_id             uuid        PRIMARY KEY REFERENCES drive.file (id) ON DELETE CASCADE,
    version             bigint      NOT NULL CHECK (version >= 1),
    state               text        NOT NULL CHECK (state IN ('processing', 'ready', 'failed')),
    job_id              uuid        NOT NULL UNIQUE,
    step_index          integer     NOT NULL CHECK (step_index >= 0),
    trigger             text        CHECK (trigger IS NULL OR trigger IN ('upload', 'reprocess', 'regenerate_page')),
    triggered_by_id     uuid,
    triggered_by_name   text,
    job_created_at      timestamptz NOT NULL,
    error_code          text,
    error_message       text,
    plan                text[],
    plan_index          integer,
    plan_label          text,
    plan_at             timestamptz,
    cancel_requested_at timestamptz,
    past_job_ids        uuid[]      NOT NULL DEFAULT '{}',
    updated_at          timestamptz NOT NULL,
    CHECK (state <> 'failed' OR error_code IS NOT NULL),
    CHECK (state = 'failed' OR (error_code IS NULL AND error_message IS NULL))
);

CREATE INDEX file_processing_past_jobs_idx ON drive.file_processing USING gin (past_job_ids);
CREATE INDEX file_processing_triggered_by_idx ON drive.file_processing (triggered_by_id)
    WHERE triggered_by_id IS NOT NULL;

-- The last change of each object, read from its facts as 0.4.0 did: its
-- latest fact, else its creation (a page: its file's).
ALTER TABLE drive.file ADD COLUMN updated_at timestamptz, ADD COLUMN version bigint;
UPDATE drive.file f
SET updated_at = COALESCE(
        (SELECT x.occurred_at FROM drive.fact x
         WHERE x.aggregate_type = 'file' AND x.aggregate_id = f.id AND x.page_number IS NULL
         ORDER BY x.occurred_at DESC, x.id DESC LIMIT 1),
        f.created_at),
    version = 1;
ALTER TABLE drive.file
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN version SET NOT NULL,
    ADD CONSTRAINT file_version_positive CHECK (version >= 1);

ALTER TABLE drive.file_page
    ADD COLUMN updated_at timestamptz,
    ADD COLUMN updated_by uuid,
    ADD COLUMN version bigint;
UPDATE drive.file_page p
SET updated_at = f.created_at,
    updated_by = f.created_by,
    version = 1
FROM drive.file f
WHERE f.id = p.file_id;
UPDATE drive.file_page p
SET updated_at = last.occurred_at,
    updated_by = last.actor_id
FROM (
    SELECT DISTINCT ON (x.aggregate_id, x.page_number)
           x.aggregate_id AS file_id, x.page_number AS number, x.occurred_at, x.actor_id
    FROM drive.fact x
    WHERE x.aggregate_type = 'page'
    ORDER BY x.aggregate_id, x.page_number, x.occurred_at DESC, x.id DESC
) last
WHERE last.file_id = p.file_id AND last.number = p.number;
ALTER TABLE drive.file_page
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN version SET NOT NULL,
    ADD CONSTRAINT file_page_version_positive CHECK (version >= 1);

ALTER TABLE drive.label ADD COLUMN updated_at timestamptz, ADD COLUMN version bigint;
UPDATE drive.label l
SET updated_at = COALESCE(
        (SELECT x.occurred_at FROM drive.fact x
         WHERE x.aggregate_type = 'label' AND x.aggregate_id = l.id
         ORDER BY x.occurred_at DESC, x.id DESC LIMIT 1),
        l.created_at),
    version = 1;
ALTER TABLE drive.label
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN version SET NOT NULL,
    ADD CONSTRAINT label_version_positive CHECK (version >= 1);

ALTER TABLE drive.ruleset ADD COLUMN updated_at timestamptz, ADD COLUMN version bigint;
UPDATE drive.ruleset r
SET updated_at = COALESCE(
        (SELECT x.occurred_at FROM drive.fact x
         WHERE x.aggregate_type = 'ruleset' AND x.aggregate_id = r.id
         ORDER BY x.occurred_at DESC, x.id DESC LIMIT 1),
        r.created_at),
    version = 1;
ALTER TABLE drive.ruleset
    ALTER COLUMN updated_at SET NOT NULL,
    ALTER COLUMN version SET NOT NULL,
    ADD CONSTRAINT ruleset_version_positive CHECK (version >= 1);

-- The processing state of every file that ever had a job, from its last job
-- as the view computed it. Its last change is the file's: the file's
-- `updatedAt` (the latest of both rows) reads as 0.4.0 showed it.
INSERT INTO drive.file_processing (
    file_id, version, state, job_id, step_index, trigger, triggered_by_id, triggered_by_name,
    job_created_at, error_code, error_message, plan, plan_index, plan_label, plan_at,
    cancel_requested_at, past_job_ids, updated_at)
SELECT s.file_id,
       1,
       s.processing_state,
       s.last_job_id,
       s.last_job_step,
       s.last_job_trigger,
       s.last_job_triggered_by_id,
       s.last_job_triggered_by_name,
       s.last_job_created_at,
       CASE WHEN s.processing_state = 'failed' THEN s.processing_error END,
       CASE WHEN s.processing_state = 'failed' THEN e.message END,
       s.last_job_plan,
       s.last_job_plan_index,
       s.last_job_plan_label,
       s.last_job_plan_at,
       s.last_job_cancel_requested_at,
       ARRAY(SELECT j.job_id FROM drive.file_job j
             WHERE j.file_id = s.file_id AND j.job_id <> s.last_job_id
             ORDER BY j.number),
       f.updated_at
FROM drive.file_status s
JOIN drive.file f ON f.id = s.file_id
LEFT JOIN drive.file_job_end e ON e.job_id = s.last_job_id
WHERE s.last_job_id IS NOT NULL;

-- The library's fact tables go, rows included.
DROP VIEW drive.file_status;
DROP TABLE drive.file_job_step;
DROP TABLE drive.file_job_plan;
DROP TABLE drive.file_job_cancel;
DROP TABLE drive.file_job_end;
DROP TABLE drive.file_job;
DROP TABLE drive.fact;
