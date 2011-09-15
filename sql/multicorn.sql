-- create wrapper with validator and handler
CREATE OR REPLACE FUNCTION multicorn_validator (text[], oid)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION multicorn_handler ()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER multicorn
VALIDATOR multicorn_validator HANDLER multicorn_handler;
