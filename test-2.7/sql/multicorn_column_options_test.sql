CREATE EXTENSION multicorn;
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE user mapping FOR current_user server multicorn_srv options (usermapping 'test');

CREATE foreign table testmulticorn (
    test1 character varying options (prefix 'test'),
    test2 character varying
) server multicorn_srv options (
    option1 'option1'
);

select * from testmulticorn limit 1;

ALTER foreign table testmulticorn alter test1 options (set prefix 'test2');

select * from testmulticorn limit 1;

ALTER foreign table testmulticorn alter test1 options (drop prefix);

select * from testmulticorn limit 1;

ALTER foreign table testmulticorn alter test1 options (add prefix 'test3');

select * from testmulticorn limit 1;
DROP USER MAPPING FOR current_user SERVER multicorn_srv;
DROP EXTENSION multicorn cascade;
