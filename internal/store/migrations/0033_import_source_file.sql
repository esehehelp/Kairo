-- An import of one regular file (a published checkpoint, kairo data import FILE): an input is a directory, so such a
-- source is not read in place but brought (a directory holding the file), as on any other node (issue #27).
ALTER TABLE data_objects ADD COLUMN source_file BOOLEAN NOT NULL DEFAULT false;
