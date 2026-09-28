-- What the workers produce leaves the file row: `drive.file_processed` holds a
-- file's results, and only them — the indexer's triple, and (through their
-- foreign keys) the pages and the extracted images. No state, no job, no rule:
-- the file keeps its plan (`ruleset_id`, `steps`) and its facts
-- (`committed_at`), the job log says where processing is. The row is created by
-- the first write that needs it (a runner's report or image, an import, a page
-- edit) and goes with its file.
CREATE TABLE drive.file_processed (
    file_id          uuid    PRIMARY KEY REFERENCES drive.file (id) ON DELETE CASCADE,
    summary          text,
    page_count       integer CHECK (page_count IS NULL OR page_count >= 0),
    estimated_tokens bigint  CHECK (estimated_tokens IS NULL OR estimated_tokens >= 0)
);

-- Every file holding any result keeps it.
INSERT INTO drive.file_processed (file_id, summary, page_count, estimated_tokens)
SELECT f.id, f.summary, f.page_count, f.estimated_tokens
FROM drive.file f
WHERE f.summary IS NOT NULL
   OR f.page_count IS NOT NULL
   OR f.estimated_tokens IS NOT NULL
   OR EXISTS (SELECT 1 FROM drive.file_page p WHERE p.file_id = f.id)
   OR EXISTS (SELECT 1 FROM drive.file_image i WHERE i.file_id = f.id);

ALTER TABLE drive.file_page
    DROP CONSTRAINT file_page_file_id_fkey,
    ADD CONSTRAINT file_page_file_id_fkey
        FOREIGN KEY (file_id) REFERENCES drive.file_processed (file_id) ON DELETE CASCADE;

ALTER TABLE drive.file_image
    DROP CONSTRAINT file_image_file_id_fkey,
    ADD CONSTRAINT file_image_file_id_fkey
        FOREIGN KEY (file_id) REFERENCES drive.file_processed (file_id) ON DELETE CASCADE;

ALTER TABLE drive.file
    DROP COLUMN summary,
    DROP COLUMN page_count,
    DROP COLUMN estimated_tokens;
