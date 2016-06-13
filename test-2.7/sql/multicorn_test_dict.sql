SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
CREATE EXTENSION hstore;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE user mapping for postgres server multicorn_srv options (usermapping 'test');

CREATE foreign table testmulticorn (
    test1 hstore,
    test2 hstore
) server multicorn_srv options (
    option1 'option1',
    test_type 'dict'
);

-- Test "normal" usage
select * from testmulticorn;

select test1 -> 'repeater' as r from testmulticorn order by r;
DROP USER MAPPING FOR postgres SERVER multicorn_srv;
DROP EXTENSION multicorn cascade;
