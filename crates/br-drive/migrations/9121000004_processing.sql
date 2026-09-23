ALTER TABLE drive.file
    ADD COLUMN ruleset_id       uuid,
    ADD COLUMN steps            jsonb,
    ADD COLUMN step_index       integer CHECK (step_index IS NULL OR step_index >= 0),
    ADD COLUMN step_count       integer CHECK (step_count IS NULL OR step_count >= 1),
    ADD COLUMN step_runner_type text    CHECK (step_runner_type IS NULL OR octet_length(step_runner_type) BETWEEN 1 AND 128),
    ADD COLUMN job_id           uuid,
    ADD COLUMN plan             text[],
    ADD COLUMN progress_index   integer CHECK (progress_index IS NULL OR progress_index >= 0),
    ADD COLUMN progress_label   text,
    ADD COLUMN progress_at      timestamptz,
    ADD COLUMN triggered_by     jsonb,
    ADD CONSTRAINT file_job_only_while_processing
        CHECK (job_id IS NULL OR processing_state = 'processing');

CREATE UNIQUE INDEX file_job_idx ON drive.file (job_id) WHERE job_id IS NOT NULL;

CREATE TABLE drive.ruleset (
    id          uuid        PRIMARY KEY,
    name        text        NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    trigger     text        NOT NULL CHECK (trigger IN ('upload', 'reprocess', 'regenerate_page')),
    media_types text[]      NOT NULL CHECK (cardinality(media_types) >= 1),
    steps       jsonb       NOT NULL,
    is_default  boolean     NOT NULL DEFAULT false,
    created_by  uuid        NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE UNIQUE INDEX ruleset_name_idx ON drive.ruleset (lower(name));

CREATE TABLE drive.known_runner_type (
    runner_type text        PRIMARY KEY CHECK (octet_length(runner_type) BETWEEN 1 AND 128),
    lifecycle   text        NOT NULL CHECK (lifecycle IN ('active', 'deprecated')),
    version     integer     NOT NULL,
    seen_at     timestamptz NOT NULL
);
