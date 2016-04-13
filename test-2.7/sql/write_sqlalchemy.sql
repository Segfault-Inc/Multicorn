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

create foreign table testalchemy (
  id integer,
  adate date,
  atimestamp timestamp,
  anumeric numeric,
  avarchar varchar
) server multicorn_srv options (
  tablename 'basetable'
);

create table basetable (
  id integer primary key,
  adate date,
  atimestamp timestamp,
  anumeric numeric,
  avarchar varchar
);

insert into testalchemy (id, adate, atimestamp, anumeric, avarchar) values 
  (1, '1980-01-01', '1980-01-01  11:01:21.132912', 3.4, 'Test');

ALTER FOREIGN TABLE testalchemy OPTIONS (ADD primary_key 'id');


BEGIN;

insert into testalchemy (id, adate, atimestamp, anumeric, avarchar) values 
  (1, '1980-01-01', '1980-01-01  11:01:21.132912', 3.4, 'Test'),
  (2, '1990-03-05', '1998-03-02  10:40:18.321023', 12.2, 'Another Test'),
  (3, '1972-01-02', '1972-01-02  16:12:54', 4000, 'another Test'),
  (4, '1922-11-02', '1962-01-02  23:12:54', -3000, NULL);

select * from basetable;

ROLLBACK;

BEGIN;


insert into testalchemy (id, adate, atimestamp, anumeric, avarchar) values 
  (1, '1980-01-01', '1980-01-01  11:01:21.132912', 3.4, 'Test'),
  (2, '1990-03-05', '1998-03-02  10:40:18.321023', 12.2, 'Another Test'),
  (3, '1972-01-02', '1972-01-02  16:12:54', 4000, 'another Test'),
  (4, '1922-11-02', '1962-01-02  23:12:54', -3000, NULL);

update testalchemy set avarchar = avarchar || ' UPDATED!';

COMMIT;

SELECT * from basetable;

DELETE from testalchemy;

SELECT * from basetable;

SET client_min_messages=WARNING;
DROP EXTENSION multicorn cascade;
DROP TABLE basetable;
