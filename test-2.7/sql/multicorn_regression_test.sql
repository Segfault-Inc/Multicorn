SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE user mapping for postgres server multicorn_srv options (usermapping 'test');

CREATE foreign table testmulticorn (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1'
);

-- Test "normal" usage
select * from testmulticorn;

-- Test quals
select * from testmulticorn where test1 like '%0';
select * from testmulticorn where test1 ilike '%0';


-- Test columns
select test2 from testmulticorn;

-- Test subquery plan
select test1, (select max(substr(test1, 9, 1))::int as max from testmulticorn t2 where substr(t2.test1, 7, 1)::int = substr(t1.test1, 7, 1)::int) as max
from testmulticorn t1 order by max desc;

select test1, (select max(substr(test1, 9, 1))::int as max from testmulticorn t2 where t2.test1 = t1.test1) as max
from testmulticorn t1 order by max desc;

select * from testmulticorn where test1 is null;

select * from testmulticorn where test1 is not null;

select * from testmulticorn where 'grou' > test1;

select * from testmulticorn where test1 < ANY(ARRAY['grou', 'MACHIN']);

CREATE foreign table testmulticorn2 (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option2'
);

select * from testmulticorn union all select * from testmulticorn2;

create function test_function_immutable () returns varchar as $$
    BEGIN
        RETURN 'test';
    END
$$ immutable language plpgsql;

create function test_function_stable () returns varchar as $$
    BEGIN
        RETURN 'test';
    END
$$  stable language plpgsql;

create function test_function_volatile () returns varchar as $$
    BEGIN
        RETURN 'test';
    END
$$  volatile language plpgsql;

select * from testmulticorn where test1 like test_function_immutable();

select * from testmulticorn where test1 like test_function_stable();

select * from testmulticorn where test1 like test_function_volatile();

select * from testmulticorn where test1 like length(test2)::varchar;


\set FETCH_COUNT 1000
select * from testmulticorn;

-- Test that zero values are converted to zero and not null
ALTER FOREIGN TABLE testmulticorn options (add test_type 'int');
ALTER FOREIGN TABLE testmulticorn alter test1 type integer;

select * from testmulticorn limit 1;

select * from testmulticorn where test1 = 0;

ALTER FOREIGN TABLE testmulticorn options (drop test_type);

-- Test operations with bytea
ALTER FOREIGN TABLE testmulticorn alter test2 type bytea;
ALTER FOREIGN TABLE testmulticorn alter test1 type bytea;

select encode(test1, 'escape') from testmulticorn where test2 = 'test2 1 19'::bytea;

-- Test operations with None
ALTER FOREIGN TABLE testmulticorn options (add test_type 'None');

select * from testmulticorn;

ALTER FOREIGN TABLE testmulticorn options (set test_type 'iter_none');

select * from testmulticorn;

ALTER FOREIGN TABLE testmulticorn add test3 money;

SELECT * from testmulticorn where test3 = 12::money;
SELECT * from testmulticorn where test1 = '12 €';

SET client_min_messages=WARNING;
DROP USER MAPPING FOR postgres SERVER multicorn_srv;
DROP EXTENSION multicorn cascade;
