-- Setup the test
CREATE EXTENSION multicorn;
CREATE language plpython3u;
\i test-common/disable_jit.include

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
$$ language plpython3u;

CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.fsfdw.FilesystemFdw'
);


CREATE TABLE temp_dir (dirname varchar);

-- Create a table with the filesystem fdw in a temporary directory,
-- and store the dirname in the temp_dir table.

select create_table();

-- End of Setup

\i test-common/multicorn_testfilesystem.include

-- Cleanup everything we've done

CREATE OR REPLACE FUNCTION cleanup_dir() RETURNS VOID AS $$
    import shutil
    root_dir = plpy.execute("""SELECT dirname from temp_dir;""")[0]['dirname']
    shutil.rmtree(root_dir)
$$ language plpython3u;

select cleanup_dir();

DROP FUNCTION cleanup_dir();
DROP TABLE temp_dir;
DROP FUNCTION create_table();
DROP EXTENSION multicorn cascade;
DROP LANGUAGE plpython3u;
