-- Setup the test
CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.fsfdw.FilesystemFdw'
);

CREATE language plpythonu;

CREATE TABLE temp_dir (dirname varchar);

-- Create a table with the filesystem fdw in a temporary directory,
-- and store the dirname in the temp_dir table.
CREATE OR REPLACE FUNCTION create_table() RETURNS VOID AS $$
    import plpy
    import tempfile
    import os
    dir = tempfile.mkdtemp()
    plpy.execute("""
        INSERT INTO temp_dir(dirname) VALUES ('%s')
    """ % str(dir))
    plpy.execute("""
    CREATE foreign table testmulticorn (
        color varchar,
        size varchar,
        name varchar,
        ext varchar,
        filename varchar,
        data varchar
    ) server multicorn_srv options (
        filename_column 'filename',
        content_column 'data',
        pattern '{color}/{size}/{name}.{ext}',
        root_dir '%s'
    );
    """ % dir)
    for color in ('blue', 'red'):
        for size in ('big', 'small'):
            dirname  = os.path.join(dir, color, size)
            os.makedirs(dirname)
            for name, ext in (('square', 'txt'), ('round', 'ini')):
                with open(os.path.join(dirname, '.'.join([name, ext])), 'a') as fd:
                    fd.write('Im a %s %s %s\n' % (size, color, name))
$$ language plpythonu;

select create_table();

-- End of Setup

-- Should have 8 lines.
SELECT * from testmulticorn;

-- Test the cost analysis
EXPLAIN select color, size from testmulticorn where color = 'blue' and size = 'big' and name = 'square' and ext = 'txt';
EXPLAIN select color, size from testmulticorn where color = 'blue' and size = 'big';
EXPLAIN select color, size from testmulticorn where color = 'blue';
EXPLAIN select color, size, data from testmulticorn where color = 'blue' and size = 'big' and name = 'square' and ext = 'txt';

-- Test insertion

-- Normal insertion
INSERT INTO testmulticorn (color, size, name, ext, data) VALUES ('yellow', 'big', 'square', 'text', 'Im a big yellow square') RETURNING filename;

-- Insertion with redundant filename/properties
INSERT INTO testmulticorn (color, size, name, ext, data, filename) VALUES ('yellow', 'small', 'square', 'txt', 'Im a small yellow square',
        'yellow/small/square.txt');

-- Insertion with just a filename
INSERT INTO testmulticorn (data, filename) VALUES ('Im a big blue triangle', 'blue/big/triangle.txt') RETURNING color, size, name, ext;

-- Should have 11 lines by now.
SELECT * from testmulticorn;


-- Insertion with incoherent filename/properties (should fail)
INSERT INTO testmulticorn (color, size, name, ext, data, filename) VALUES ('blue', 'big', 'triangle', 'txt', 'Im a big blue triangle',
        'blue/small/triangle.txt');

-- Insertion with missing keys (should fail)
INSERT INTO testmulticorn (color, size, name) VALUES ('blue', 'small', 'triangle');

-- Insertion with missing keys and filename (should fail)
INSERT INTO testmulticorn (color, size, name, filename) VALUES ('blue', 'small', 'triangle', 'blue/small/triangle.txt');

-- Insertion which would overwrite a file.
-- Normal insertion
INSERT INTO testmulticorn (color, size, name, ext, data) VALUES ('yellow', 'big', 'square', 'text', 'Im a duplicate big square');

-- Should still have 11 lines by now.
SELECT * from testmulticorn;

-- Test insertion in transaction
BEGIN;
    INSERT INTO testmulticorn (data, filename) VALUES ('Im a big red triangle', 'red/big/triangle.txt');
    SELECT * from testmulticorn where name = 'triangle' and color = 'red';
ROLLBACK;

-- The file should not be persisted.
SELECT * from testmulticorn where name = 'triangle' and color = 'red';

-- Test Update
UPDATE testmulticorn set name = 'rectangle' where name = 'square' RETURNING filename;

-- O lines
SELECT count(1) from testmulticorn where name = 'square';

-- 6 lines
SELECT count(1) from testmulticorn where name = 'rectangle';

-- Update should not work if it would override an existing file.
UPDATE testmulticorn set filename = 'blue/big/triangle.txt' where filename = 'blue/big/rectangle.txt';

-- Update should not work when setting filename column to NULL
UPDATE testmulticorn set filename = NULL where filename = 'blue/big/rectangle.txt';
-- Update should not work when setting a property column to NULL
UPDATE testmulticorn set color = NULL where filename = 'blue/big/rectangle.txt' RETURNING color;

-- Content column update.
UPDATE testmulticorn set data = 'Im an updated rectangle' where filename = 'blue/big/rectangle.txt' RETURNING data;
SELECT * from testmulticorn where filename = 'blue/big/rectangle.txt';

-- Update in transactions
BEGIN;
    UPDATE testmulticorn set name = 'square' where name = 'rectangle';
    -- O lines
    SELECT count(1) from testmulticorn where name = 'rectangle';
    -- 6 lines
    SELECT count(1) from testmulticorn where name = 'square';
ROLLBACK;

-- O lines
SELECT count(1) from testmulticorn where name = 'square';

-- 6 lines
SELECT count(1) from testmulticorn where name = 'rectangle';

BEGIN;
    UPDATE testmulticorn set data = data || ' UPDATED!';
    -- 11 lines
    SELECT count(1) from testmulticorn where data ilike '% UPDATED!';
    SELECT data from testmulticorn where data ilike '% UPDATED!' order by filename limit 1;
ROLLBACK;

-- 0 lines
SELECT count(1) from testmulticorn where data ilike '% UPDATED!';

-- Test successive update to the same files.
BEGIN;
    UPDATE testmulticorn set color = 'cyan' where filename = 'blue/big/rectangle.txt';
    -- There should be one line with cyan color, 0 with the old filename
    SELECT filename, data from testmulticorn where color = 'cyan';
    SELECT filename, data from testmulticorn where filename = 'blue/big/rectangle.txt';

    -- There should be one line with magenta, and 0 with cyan and the old
    -- filename
    UPDATE testmulticorn set color = 'magenta' where color = 'cyan';
    SELECT filename, data from testmulticorn where color = 'magenta';
    SELECT filename, data from testmulticorn where color = 'cyan';
    SELECT filename, data from testmulticorn where filename = 'blue/big/rectangle.txt';
    UPDATE testmulticorn set color = 'blue' where color = 'magenta';

    -- There should be one line with the old filename, and zero with the rest
    SELECT filename, data from testmulticorn where filename = 'blue/big/rectangle.txt';
    SELECT filename, data from testmulticorn where color = 'magenta';
    SELECT filename, data from testmulticorn where color = 'cyan';
COMMIT;

-- Result should be the same than pre-commit
SELECT filename, data from testmulticorn where filename = 'blue/big/rectangle.txt';
SELECT filename, data from testmulticorn where color = 'magenta';
SELECT filename, data from testmulticorn where color = 'cyan';


-- DELETE test

DELETE from testmulticorn where color = 'yellow' returning filename;
-- Should have no rows
select count(1) from testmulticorn where color = 'yellow';

-- DELETE in transaction
BEGIN;
    DELETE FROM testmulticorn where color = 'red' returning filename;
    select count(1) from testmulticorn where color = 'red';
ROLLBACK;
-- Should have 4 rows
select count(1) from testmulticorn where color = 'red';


-- Test various combinations of INSERT/UPDATE/DELETE

BEGIN;
    INSERT INTO testmulticorn (color, size, name, ext, data) VALUES
        ('cyan', 'medium', 'triangle', 'jpg', 'Im a triangle');
    INSERT INTO testmulticorn (color, size, name, ext, data) VALUES
        ('cyan', 'large', 'triangle', 'jpg', 'Im a triangle');
    -- 2 lines
    SELECT * from testmulticorn where color = 'cyan';
    UPDATE testmulticorn set color = 'magenta' where size = 'large' and color = 'cyan' returning filename;
    -- 2 lines, one cyan, one magenta
    SELECT * from testmulticorn where color in ('cyan', 'magenta');
    UPDATE testmulticorn set data = 'Im magenta' where color = 'magenta';
    DELETE FROM testmulticorn where color = 'cyan' RETURNING filename;
    -- One magenta line
    SELECT * from testmulticorn where color in ('cyan', 'magenta');
COMMIT;
-- Result should be the same as precommit
SELECT * from testmulticorn where color in ('cyan', 'magenta');

DELETE from testmulticorn where color = 'magenta';

-- Same as before, but rollbacking

BEGIN;
    INSERT INTO testmulticorn (color, size, name, ext, data) VALUES
        ('cyan', 'medium', 'triangle', 'jpg', 'Im a triangle');
    INSERT INTO testmulticorn (color, size, name, ext, data) VALUES
        ('cyan', 'large', 'triangle', 'jpg', 'Im a triangle');
    -- 2 lines
    SELECT * from testmulticorn where color = 'cyan';
    UPDATE testmulticorn set color = 'magenta' where size = 'large' and color = 'cyan' returning filename;
    -- 2 lines, one cyan, one magenta
    SELECT * from testmulticorn where color in ('cyan', 'magenta');
    UPDATE testmulticorn set data = 'Im magenta' where color = 'magenta';
    DELETE FROM testmulticorn where color = 'cyan' RETURNING filename;
    -- One magenta line
    SELECT * from testmulticorn where color in ('cyan', 'magenta');
ROLLBACK;

SELECT * from testmulticorn where color in ('cyan', 'magenta');

-- Cleanup everything we've done

CREATE OR REPLACE FUNCTION cleanup_dir() RETURNS VOID AS $$
    import shutil
    root_dir = plpy.execute("""SELECT dirname from temp_dir;""")[0]['dirname']
    shutil.rmtree(root_dir)
$$ language plpythonu;

select cleanup_dir();

DROP FUNCTION cleanup_dir();
DROP TABLE temp_dir;
DROP FUNCTION create_table();
DROP EXTENSION multicorn cascade;
DROP LANGUAGE plpythonu;
