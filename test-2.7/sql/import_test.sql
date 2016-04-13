SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE SCHEMA import_dest1;
IMPORT FOREIGN SCHEMA import_source FROM SERVER multicorn_srv INTO import_dest1;
SELECT relname FROM pg_class c INNER JOIN pg_namespace n on c.relnamespace = n.oid WHERE n.nspname = 'import_dest1';
DROP SCHEMA import_dest1 CASCADE;
CREATE SCHEMA import_dest1;
IMPORT FOREIGN SCHEMA import_source EXCEPT (imported_table_1, imported_table_3) FROM SERVER multicorn_srv INTO import_dest1;
SELECT relname FROM pg_class c INNER JOIN pg_namespace n on c.relnamespace = n.oid WHERE n.nspname = 'import_dest1';
IMPORT FOREIGN SCHEMA import_source LIMIT TO (imported_table_1) FROM SERVER multicorn_srv INTO import_dest1;
SELECT relname FROM pg_class c INNER JOIN pg_namespace n on c.relnamespace = n.oid WHERE n.nspname = 'import_dest1';

SET client_min_messages=WARNING;
DROP EXTENSION multicorn cascade;
