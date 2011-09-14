drop extension dummy_fdw cascade;
create extension dummy_fdw;
create server dummy foreign data wrapper dummy_fdw;
create foreign table test (field1 numeric, field2 character varying, field3 date)
server dummy
options (wrapper 'fdw.csvfdw.CsvFdw', filename '/tmp/test.csv', format 'csv', delimiter ',');
select * from test;

drop extension dummy_fdw cascade;
create extension dummy_fdw;
create server dummy foreign data wrapper dummy_fdw;
create foreign table ldaptest (cn character varying, sn character varying, givenName character varying)
server dummy
options (wrapper 'fdw.ldapfdw.LdapFdw', address 'ldap.keleos.fr');
select * from ldaptest;

drop extension dummy_fdw cascade;
create extension dummy_fdw;
create server dummy foreign data wrapper dummy_fdw;
create foreign table test (field1 character varying, field2 character varying, field3 character varying)
server dummy
options (wrapper 'fdw.fsfdw.FilesystemFdw', root_dir '/tmp/data', pattern '{field1}/{field2}/{field3}/style.css');
select * from test;

