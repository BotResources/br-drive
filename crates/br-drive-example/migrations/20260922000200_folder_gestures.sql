CREATE TABLE workspace_folder_gesture (
    id           uuid        PRIMARY KEY,
    workspace_id uuid        NOT NULL,
    gesture      text        NOT NULL,
    prefix       text        NOT NULL,
    new_prefix   text,
    at           timestamptz NOT NULL
);

CREATE INDEX workspace_folder_gesture_workspace_idx ON workspace_folder_gesture (workspace_id);
