drop extension dummy_fdw cascade;
create extension dummy_fdw;
create server dummy foreign data wrapper dummy_fdw;
create foreign table test (field1 numeric) server dummy;


