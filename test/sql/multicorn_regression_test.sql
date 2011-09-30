CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn;

CREATE foreign table testmulticorn (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);

select * from testmulticorn;
