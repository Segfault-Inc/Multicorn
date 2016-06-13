SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
create or replace function create_foreign_server() returns void as $block$
  DECLARE
    current_db varchar;
  BEGIN
    SELECT into current_db current_database();
    EXECUTE $$ 
    CREATE server multicorn_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.sqlalchemyfdw.SqlAlchemyFdw',
        db_url 'postgresql://$$ || current_user || '@localhost/' || current_db || $$'
    );
    $$;
  END;
$block$ language plpgsql;
select create_foreign_server();

CREATE SCHEMA local_schema;
CREATE TABLE local_schema.t1 (
  c1 int primary key,
  c2 text,
  c3 timestamp,
  c4 numeric
);

CREATE TABLE local_schema.t2 (
  c1 int,
  c2 text,
  c3 timestamp,
  c4 numeric
);

CREATE TABLE local_schema.t3 (
  c1 int,
  c2 text,
  c3 timestamp,
  c4 numeric
);


CREATE SCHEMA remote_schema;

IMPORT FOREIGN SCHEMA local_schema FROM SERVER multicorn_srv INTO remote_schema ;
\d remote_schema.t1
\d remote_schema.t2
\d remote_schema.t3
SELECT * FROM remote_schema.t1;
INSERT INTO remote_schema.t1 VALUES (1, '2', NULL, NULL);
SELECT * FROM remote_schema.t1;
DROP SCHEMA remote_schema CASCADE;
CREATE SCHEMA remote_schema;
IMPORT FOREIGN SCHEMA local_schema LIMIT TO (t1) FROM SERVER multicorn_srv INTO remote_schema ;
SELECT relname FROM pg_class c INNER JOIN pg_namespace n on c.relnamespace = n.oid WHERE n.nspname = 'remote_schema';
IMPORT FOREIGN SCHEMA local_schema EXCEPT (t1, t3) FROM SERVER multicorn_srv INTO remote_schema ;
SELECT relname FROM pg_class c INNER JOIN pg_namespace n on c.relnamespace = n.oid WHERE n.nspname = 'remote_schema';
DROP EXTENSION multicorn CASCADE;
DROP SCHEMA local_schema CASCADE;
DROP SCHEMA remote_schema CASCADE;
