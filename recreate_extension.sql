drop extension multicorn cascade;
create extension multicorn;
create server multicorn_srv foreign data wrapper multicorn;
create foreign table csvtest (
       field1 numeric,
       field2 character varying,
       field3 date
) server multicorn_srv options (
       wrapper 'multicorn.csvfdw.CsvFdw',
       filename '/tmp/test.csv',
       format 'csv',
       delimiter ',');
select * from csvtest;
select * from csvtest where field2 = 'test';

drop extension multicorn cascade;
create extension multicorn;
create server multicorn_srv foreign data wrapper multicorn;
create foreign table ldaptest (
       cn character varying,
       sn character varying,
       uid character varying,
       uidNumber int,
       o character varying,
       mail character varying
) server multicorn_srv options (
       wrapper 'multicorn.ldapfdw.LdapFdw',
       address 'ldap.keleos.fr',
       "path" 'ou=People,dc=keleos,dc=fr',
       objectclass 'inetOrgPerson');
select * from ldaptest;
select * from ldaptest where uid = 'gayoub';

drop extension multicorn cascade;
create extension multicorn;
create server multicorn_srv foreign data wrapper multicorn;
create foreign table fstest (
       field1 character varying collate "fr_FR.utf8",
       field2 character varying collate "fr_FR.utf8",
       field3 character varying collate "fr_FR.utf8"
) server multicorn_srv options (
       wrapper 'multicorn.fsfdw.FilesystemFdw',
       root_dir '/tmp/data',
       pattern '{field1}/{field2}/{field3}/style.css');
select * from fstest;
select * from fstest where field2 = 'test';

