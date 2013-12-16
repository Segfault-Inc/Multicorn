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

-- No savepoints
BEGIN;

CREATE foreign table testmulticorn_write (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test1',
    test_type 'returning'
);

insert into testmulticorn_write(test1, test2) VALUES ('0', 'A');

update testmulticorn_write set test2 = 'B' where test1 = '0';

update testmulticorn_write set test2 = 'C' where test1 = '1';

delete from testmulticorn_write where test1 = '1';

DROP foreign table testmulticorn_write;

ROLLBACK;

-- One savepoint
BEGIN; 

CREATE foreign table testmulticorn_write (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test1',
    test_type 'returning'
);

SAVEPOINT A;

insert into testmulticorn_write(test1, test2) VALUES ('0', 'A');

update testmulticorn_write set test2 = 'B' where test1 = '0';

update testmulticorn_write set test2 = 'C' where test1 = '1';

delete from testmulticorn_write where test1 = '1';

ROLLBACK TO A;

RELEASE A;

DROP foreign table testmulticorn_write;

COMMIT;

-- Multiple sequential savepoints
BEGIN; 

CREATE foreign table testmulticorn_write (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test1',
    test_type 'returning'
);

SAVEPOINT A;

insert into testmulticorn_write(test1, test2) VALUES ('0', 'A');

select * from testmulticorn LIMIT 1;

ROLLBACK TO A;

RELEASE A;

SAVEPOINT B;

update testmulticorn_write set test2 = 'B' where test1 = '0';

RELEASE B;

update testmulticorn_write set test2 = 'C' where test1 = '1';

delete from testmulticorn_write where test1 = '1';

DROP foreign table testmulticorn_write;

ROLLBACK;

-- Multiple nested savepoints
BEGIN; 

CREATE foreign table testmulticorn_write (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    row_id_column 'test1',
    test_type 'returning'
);

SAVEPOINT A;

insert into testmulticorn_write(test1, test2) VALUES ('0', 'A');

select * from testmulticorn LIMIT 1;

SAVEPOINT B;

update testmulticorn_write set test2 = 'B' where test1 = '0';

RELEASE B;

update testmulticorn_write set test2 = 'C' where test1 = '1';

delete from testmulticorn_write where test1 = '1';

ROLLBACK TO A;

RELEASE A;

DROP foreign table testmulticorn_write;

ROLLBACK;

-- Clean up
DROP EXTENSION multicorn cascade;
