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
  id integer,
  adate date,
  atimestamp timestamp,
  anumeric numeric,
  avarchar varchar
);

insert into basetable (id, adate, atimestamp, anumeric, avarchar) values 
  (1, '1980-01-01', '1980-01-01  11:01:21.132912', 3.4, 'Test'),
  (2, '1990-03-05', '1998-03-02  10:40:18.321023', 12.2, 'Another Test'),
  (3, '1972-01-02', '1972-01-02  16:12:54', 4000, 'another Test'),
  (4, '1922-11-02', '1962-01-02  23:12:54', -3000, NULL);

select * from testalchemy;

select id, adate from testalchemy;

select * from testalchemy where avarchar is null;

select * from testalchemy where avarchar is not null;

select * from testalchemy where adate > '1970-01-02'::date;

select * from testalchemy where adate between '1970-01-01' and '1980-01-01';

select * from testalchemy where anumeric > 0;

select * from testalchemy where avarchar not like '%test';

select * from testalchemy where avarchar like 'Another%';

select * from testalchemy where avarchar ilike 'Another%';

select * from testalchemy where avarchar not ilike 'Another%';

select * from testalchemy where id in (1,2);

select * from testalchemy where id not in (1, 2);

select * from testalchemy order by avarchar;

select * from testalchemy order by avarchar desc;

select * from testalchemy order by avarchar desc nulls first;

select * from testalchemy order by avarchar desc nulls last;

select * from testalchemy order by avarchar nulls first;

select * from testalchemy order by avarchar nulls last;

select count(*) from testalchemy;

DROP EXTENSION multicorn cascade;
DROP table basetable;
