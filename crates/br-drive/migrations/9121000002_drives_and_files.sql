CREATE TABLE drive.drive (
    id         uuid        PRIMARY KEY,
    created_by uuid        NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE drive.file (
    id               uuid        PRIMARY KEY,
    drive_id         uuid        NOT NULL REFERENCES drive.drive (id),
    path             text        NOT NULL,
    name             text        NOT NULL,
    protected        boolean     NOT NULL DEFAULT false,
    media_type       text        NOT NULL,
    size_bytes       bigint      NOT NULL,
    sha256           bytea       NOT NULL,
    blob_ref         uuid        NOT NULL,
    processing_state text        NOT NULL,
    processing_error text,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_by       uuid        NOT NULL,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    UNIQUE (drive_id, path, name)
);

CREATE INDEX file_drive_idx ON drive.file (drive_id);
CREATE INDEX file_blob_idx ON drive.file (blob_ref);
