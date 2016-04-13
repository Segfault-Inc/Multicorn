SET client_min_messages=NOTICE;
CREATE EXTENSION multicorn;
-- Test that the wrapper option is required on the server.
CREATE server multicorn_srv foreign data wrapper multicorn;
-- Test that the wrapper option cannot be altered on the table
CREATE server multicorn_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.testfdw.TestForeignDataWrapper'
);
CREATE foreign table testmulticorn (
    test1 character varying,
    test2 character varying
) server multicorn_srv options (
    option1 'option1',
    wrapper 'multicorn.evilwrapper.EvilDataWrapper'
);

ALTER server multicorn_srv options (DROP wrapper);

CREATE server multicorn_empty_srv foreign data wrapper multicorn;

SET client_min_messages=WARNING;
DROP EXTENSION multicorn cascade;
