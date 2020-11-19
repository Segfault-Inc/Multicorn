SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE user mapping FOR current_user server multicorn_srv options (usermapping 'test');

CREATE foreign table testmulticorn (
    test1 date,
    test2 timestamp
) server multicorn_srv options (
    option1 'option1',
    test_type 'date'
);

-- Test "normal" usage
select * from testmulticorn;

select * from testmulticorn where test1 < '2011-06-01';

select * from testmulticorn where test2 < '2011-06-01 00:00:00';
DROP USER MAPPING FOR current_user SERVER multicorn_srv;
DROP EXTENSION multicorn cascade;
