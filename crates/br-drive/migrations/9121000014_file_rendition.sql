-- A file's renditions: binary files a runner derived from the source and
-- handed back (a database dump converted into a `.duckdb` file, a Parquet
-- export, a PDF), each an object of the host's storage (`drive_rendition`
-- blob kind), one row per `(file_id, name)`. The markdown pages are the
-- file's markdown rendition and stay in `drive.file_page`.
-- Shaped like `drive.file_image` without `page`, plus `kind`. A name is
-- replaced only when its new object lands: until then the pending_* columns
-- carry the upload in flight and the landed object stays current.
-- The row goes with its file; its objects do not: every path that deletes a
-- file releases them explicitly, in its own transaction (a cascade is never
-- seen by the engine's blob accounting).
-- Additive: no existing table or row changes; a 0.7.x binary ignores it.
CREATE TABLE drive.file_rendition (
    file_id            uuid        NOT NULL REFERENCES drive.file (id) ON DELETE CASCADE,
    name               text        NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 64
                                                   AND name ~ '^[a-z0-9][a-z0-9._-]*$'),
    kind               text        NOT NULL CHECK (octet_length(kind) BETWEEN 1 AND 32
                                                   AND kind ~ '^[a-z][a-z0-9_]*$'),
    blob_ref           uuid        NOT NULL,
    media_type         text        NOT NULL CHECK (octet_length(media_type) BETWEEN 3 AND 255),
    size_bytes         bigint      NOT NULL CHECK (size_bytes >= 0),
    sha256             bytea       NOT NULL CHECK (octet_length(sha256) = 32),
    landed_at          timestamptz,
    pending_blob_ref   uuid,
    pending_media_type text        CHECK (pending_media_type IS NULL OR octet_length(pending_media_type) BETWEEN 3 AND 255),
    pending_size_bytes bigint      CHECK (pending_size_bytes IS NULL OR pending_size_bytes >= 0),
    pending_sha256     bytea       CHECK (pending_sha256 IS NULL OR octet_length(pending_sha256) = 32),
    pending_kind       text        CHECK (pending_kind IS NULL OR (octet_length(pending_kind) BETWEEN 1 AND 32
                                                                   AND pending_kind ~ '^[a-z][a-z0-9_]*$')),
    requested_at       timestamptz NOT NULL,
    PRIMARY KEY (file_id, name),
    CHECK ((pending_blob_ref IS NULL) = (pending_media_type IS NULL)),
    CHECK ((pending_blob_ref IS NULL) = (pending_size_bytes IS NULL)),
    CHECK ((pending_blob_ref IS NULL) = (pending_sha256 IS NULL)),
    CHECK ((pending_blob_ref IS NULL) = (pending_kind IS NULL))
);

CREATE INDEX file_rendition_blob_idx ON drive.file_rendition (blob_ref);
CREATE INDEX file_rendition_pending_blob_idx ON drive.file_rendition (pending_blob_ref);
