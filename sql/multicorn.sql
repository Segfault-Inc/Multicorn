-- create wrapper with validator and handler
--
CREATE OR REPLACE FUNCTION multicorn_check_plpython3u() RETURNS VOID AS $$
DECLARE 
res bool := false; 
BEGIN
  SELECT count(1) > 0 INTO res FROM pg_language WHERE lanname = 'plpython3u'; 
  IF res THEN
    DO $py$ import plpy; 2+2 $py$ language plpython3u;
  END IF;
END 
$$ language plpgsql VOLATILE;

SELECT multicorn_check_plpython3u();

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
