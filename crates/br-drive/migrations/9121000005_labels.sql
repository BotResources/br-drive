CREATE TABLE drive.label (
    id          uuid        PRIMARY KEY,
    name        text        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 100),
    color       text        NOT NULL CHECK (color ~ '^#[0-9a-f]{6}$'),
    description text        NOT NULL DEFAULT '' CHECK (octet_length(description) <= 1024),
    created_by  uuid        NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE UNIQUE INDEX label_name_idx ON drive.label (lower(name));

CREATE TABLE drive.file_label (
    file_id    uuid        NOT NULL REFERENCES drive.file (id) ON DELETE CASCADE,
    label_id   uuid        NOT NULL REFERENCES drive.label (id) ON DELETE CASCADE,
    created_by uuid        NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (file_id, label_id)
);

CREATE INDEX file_label_label_idx ON drive.file_label (label_id);
