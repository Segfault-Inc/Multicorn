CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE user mapping for postgres server multicorn_srv options (usermapping 'test');

CREATE foreign table testmulticorn (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    test_type 'nowrite'
);

insert into testmulticorn(test1, test2) VALUES ('test', 'test2');

update testmulticorn set test1 = 'test';

delete from testmulticorn where test2 = 'test2 2 0';

CREATE foreign table testmulticorn_write (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test1',
	test_type 'returning'
);

insert into testmulticorn_write(test1, test2) VALUES ('test', 'test2');

update testmulticorn_write set test1 = 'test' where test1 ilike 'test1 3%';

delete from testmulticorn_write where test2 = 'test2 2 0';

-- Test returning
insert into testmulticorn_write(test1, test2) VALUES ('test', 'test2') RETURNING test1;

update testmulticorn_write set test1 = 'test' where test1 ilike 'test1 3%' RETURNING test1;

DROP foreign table testmulticorn_write;
-- Now test with another column
CREATE foreign table testmulticorn_write(
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test2'
);

insert into testmulticorn_write(test1, test2) VALUES ('test', 'test2');

update testmulticorn_write set test1 = 'test' where test1 ilike 'test1 3%';

delete from testmulticorn_write where test2 = 'test2 2 0';

update testmulticorn_write set test2 = 'test' where test2 = 'test2 1 1';

DROP foreign table testmulticorn_write;
-- Now test with other types
CREATE foreign table testmulticorn_write(
    test1 date,
    test2 timestamp
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test2',
	test_type 'date'
);

insert into testmulticorn_write(test1, test2) VALUES ('2012-01-01', '2012-01-01 00:00:00');

delete from testmulticorn_write where test2 > '2011-12-03';

update testmulticorn_write set test1 = date_trunc('day', test1) where test2 = '2011-09-03 14:30:25';

DROP foreign table testmulticorn_write;
-- Test with unknown column
CREATE foreign table testmulticorn_write(
    test1 date,
    test2 timestamp
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'teststuff',
	test_type 'date'
);
delete from testmulticorn_write;

DROP EXTENSION multicorn cascade;
