/*
 * Author: Dickson S. Guedes
 * Created at: 2011-07-30 13:35:30 -0300
 *
 */ 

-- create wrapper with validator and handler
CREATE OR REPLACE FUNCTION dummy_fdw_validator (text[], oid)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION dummy_fdw_handler ()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER dummy_fdw
VALIDATOR dummy_fdw_validator HANDLER dummy_fdw_handler;
