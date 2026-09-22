CREATE TABLE workspace (
    id         uuid PRIMARY KEY,
    owner_id   uuid        NOT NULL,
    name       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX workspace_owner_idx ON workspace (owner_id);
