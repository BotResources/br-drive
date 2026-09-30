-- The `upload` rule an uploader chose at `RequestUpload`, pinned on the file
-- for the commit's chain (`process_on_commit`). NULL — every file before
-- 0.5.1, and every upload naming no rule — runs the default `upload` rule
-- matching its media type, as before. No foreign key: a rule deleted before
-- the commit leaves the pin dangling, and the commit then stores the file
-- without processing it (never the default in its place). Additive: a 0.5.0
-- binary ignores the column.
ALTER TABLE drive.file ADD COLUMN upload_ruleset_id uuid;
