-- A file's human-facing title, independent of its name: at most 255
-- characters. Existing files take their name without its extension, trimmed
-- of the same Unicode white space as the library's rule, the default an
-- upload applies when it names no title. One-way: a pre-title binary cannot
-- create a file once this is applied.
ALTER TABLE drive.file ADD COLUMN title text;

UPDATE drive.file
SET title = CASE
    WHEN btrim(regexp_replace(name, '\.[^.]*$', ''), U&' \00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''
        THEN btrim(regexp_replace(name, '\.[^.]*$', ''), U&' \00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000')
    ELSE name
END;

ALTER TABLE drive.file
    ALTER COLUMN title SET NOT NULL,
    ADD CONSTRAINT file_title_length CHECK (char_length(title) BETWEEN 1 AND 255);
