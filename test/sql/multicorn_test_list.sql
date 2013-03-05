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

select test1[2] from testmulticorn;

DROP EXTENSION multicorn cascade;
