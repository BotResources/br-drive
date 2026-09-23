ALTER TABLE drive.file
    ADD COLUMN summary          text,
    ADD COLUMN page_count       integer CHECK (page_count IS NULL OR page_count >= 0),
    ADD COLUMN estimated_tokens bigint  CHECK (estimated_tokens IS NULL OR estimated_tokens >= 0);

CREATE TABLE drive.file_page (
    file_id    uuid        NOT NULL REFERENCES drive.file (id) ON DELETE CASCADE,
    number     integer     NOT NULL CHECK (number >= 1),
    markdown   text        NOT NULL,
    origin     text        NOT NULL CHECK (origin IN ('runner', 'regenerated', 'edited')),
    updated_by uuid        NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (file_id, number)
);

CREATE TABLE drive.file_image (
    file_id            uuid        NOT NULL REFERENCES drive.file (id) ON DELETE CASCADE,
    name               text        NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 64),
    page               integer     NOT NULL CHECK (page >= 1),
    blob_ref           uuid        NOT NULL,
    media_type         text        NOT NULL CHECK (octet_length(media_type) BETWEEN 3 AND 255),
    size_bytes         bigint      NOT NULL CHECK (size_bytes >= 0),
    sha256             bytea       NOT NULL CHECK (octet_length(sha256) = 32),
    landed_at          timestamptz,
    pending_blob_ref   uuid,
    pending_media_type text        CHECK (pending_media_type IS NULL OR octet_length(pending_media_type) BETWEEN 3 AND 255),
    pending_size_bytes bigint      CHECK (pending_size_bytes IS NULL OR pending_size_bytes >= 0),
    pending_sha256     bytea       CHECK (pending_sha256 IS NULL OR octet_length(pending_sha256) = 32),
    requested_at       timestamptz NOT NULL,
    PRIMARY KEY (file_id, name),
    CHECK ((pending_blob_ref IS NULL) = (pending_media_type IS NULL)),
    CHECK ((pending_blob_ref IS NULL) = (pending_size_bytes IS NULL)),
    CHECK ((pending_blob_ref IS NULL) = (pending_sha256 IS NULL))
);

CREATE INDEX file_image_blob_idx ON drive.file_image (blob_ref);
CREATE INDEX file_image_pending_blob_idx ON drive.file_image (pending_blob_ref);
