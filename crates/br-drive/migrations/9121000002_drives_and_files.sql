CREATE TABLE drive.drive (
    id         uuid        PRIMARY KEY,
    created_by uuid        NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE drive.file (
    id               uuid        PRIMARY KEY,
    drive_id         uuid        NOT NULL REFERENCES drive.drive (id),
    path             text        NOT NULL CHECK (octet_length(path) <= 1024),
    name             text        NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    protected        boolean     NOT NULL DEFAULT false,
    media_type       text        NOT NULL CHECK (octet_length(media_type) BETWEEN 3 AND 255),
    size_bytes       bigint      NOT NULL CHECK (size_bytes >= 0),
    sha256           bytea       NOT NULL CHECK (octet_length(sha256) = 32),
    blob_ref         uuid        NOT NULL,
    processing_state text        NOT NULL
        CHECK (processing_state IN ('pending', 'processing', 'ready', 'failed')),
    processing_error text,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_by       uuid        NOT NULL,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    UNIQUE (drive_id, path, name)
);

CREATE INDEX file_drive_idx ON drive.file (drive_id);
CREATE INDEX file_blob_idx ON drive.file (blob_ref);
