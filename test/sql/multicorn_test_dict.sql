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

select test1 -> 'repeater' from testmulticorn;

DROP EXTENSION multicorn cascade;
