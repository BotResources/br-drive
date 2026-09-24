-- The per-drive file counts a host view reads (`br_drive::file_counts`) come
-- from the index alone.
CREATE INDEX file_drive_state_idx ON drive.file (drive_id, processing_state);
