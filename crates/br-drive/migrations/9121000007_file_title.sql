-- A file's human-facing title, independent of its name: at most 255
-- characters. Existing files take their name without its extension, the
-- default an upload applies when it names no title.
ALTER TABLE drive.file ADD COLUMN title text;

UPDATE drive.file
SET title = CASE
    WHEN btrim(regexp_replace(name, '\.[^.]*$', '')) <> ''
        THEN btrim(regexp_replace(name, '\.[^.]*$', ''))
    ELSE name
END;

ALTER TABLE drive.file
    ALTER COLUMN title SET NOT NULL,
    ADD CONSTRAINT file_title_length CHECK (char_length(title) BETWEEN 1 AND 255);
