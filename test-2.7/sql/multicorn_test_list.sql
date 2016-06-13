SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE user mapping for postgres server multicorn_srv options (usermapping 'test');

CREATE foreign table testmulticorn (
    test1 character varying[],
    test2 character varying[]
) server multicorn_srv options (
    option1 'option1',
    test_type 'list'
);

-- Test "normal" usage
select * from testmulticorn;

select test1[2] as c from testmulticorn order by c;

alter foreign table testmulticorn alter test1 type varchar;

select * from testmulticorn;

alter foreign table testmulticorn options (set test_type 'nested_list');

select * from testmulticorn limit 1;

alter foreign table testmulticorn alter test1 type varchar[];
alter foreign table testmulticorn alter test2 type varchar[][];

select test1[2], test2[2][2], array_length(test1, 1), array_length(test2, 1), array_length(test2, 2) from testmulticorn limit 1;

select length(test1[2]) from testmulticorn limit 1;
DROP USER MAPPING FOR postgres SERVER multicorn_srv;
DROP EXTENSION multicorn cascade;
