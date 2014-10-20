.. contents::

Multicorn is bundled with a small set of Foreign Data Wrappers, which you can
use or customize for your needs.


SQLAlchemy Foreign Data Wrapper
===============================

Class: ``multicorn.sqlalchemyfdw.SqlAlchemyFdw``

Source code: `multicorn/sqlalchemyfdw.py`_

.. _multicorn/sqlalchemyfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/sqlalchemyfdw.py

Purpose
-------

This fdw can be used to access data stored in a remote RDBMS. 
Through the use of sqlalchemy, many different rdbms engines are supported.

Dependencies
------------

You will need the `sqlalchemy`_ library, as well as a suitable dbapi driver for
the remote database.

You can find a list of supported RDBMs, and their associated dbapi drivers and
connection strings in the `sqlalchemy dialects documentation`_.

.. _sqlalchemy dialects documentation: http://docs.sqlalchemy.org/en/latest/dialects/

.. _sqlalchemy: http://www.sqlalchemy.org/

Required options
----------------

``db_url`` (string)
  An sqlalchemy connection string.
  Examples:
    
    - mysql: `mysql://<user>:<password>@<host>/<dbname>`
    - mssql: `mssql://<user>:<password>@<dsname>`

  See the `sqlalchemy dialects documentation`_. for documentation.

``tablename`` (string)
  The table name in the remote RDBMS.

Allowed options
---------------

``primary_key`` (string)
  Identifies a column which is a primary key in the remote RDBMS.
  This options is required for INSERT, UPDATE and DELETE operations

When defining the table, the local column names will be used to retrieve the
remote column data.
Moreover, the local column types will be used to interpret the results in the
remote table. Sqlalchemy being aware of the differences between database
implementations, it will convert each value from the remote database to python
using the converter inferred from the column type, and convert it back to a
postgresql suitable form.

What does it do to reduce the amount of fetched data ?
------------------------------------------------------

- `quals` are pushed to the remote database whenever possible. This include
  simple operators : 
  
    - equality, inequality (=, <>, >, <, <=, >=)
    - like, ilike and their negations
    - IN clauses with scalars, = ANY (array)
    - NOT IN clauses, != ALL (array)
- the set of needed columns is pushed to the remote_side, and only those columns
  will be fetched.

Usage example
-------------

For a connection to a remote mysql database (you'll need a mysql dbapi driver,
such as pymysql):

.. code-block:: sql

  CREATE SERVER alchemy_srv foreign data wrapper multicorn options (
      wrapper 'multicorn.sqlalchemyfdw.SqlAlchemyFdw'
  );

  create foreign table mysql_table (
    column1 integer,
    column2 varchar
  ) server alchemy_srv options (
    tablename 'table',
    db_url 'mysql://myuser:mypassword@myhost/mydb'
  );


CSV Foreign Data Wrapper
========================

Class: ``multicorn.csvfdw.CsvFdw``

Source code: `multicorn/csvfdw.py`_

.. _multicorn/csvfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/csvfdw.py

Purpose
-------

This fdw can be used to access data stored in `CSV files`_. Each column defined
in the table will be mapped, in order, against columns in the CSV file.

.. _CSV files: http://en.wikipedia.org/wiki/Comma-separated_values

Dependencies
------------

No dependency outside the standard python distribution.

Required options
----------------

``filename`` (string)
  The full path to the CSV file containing the data. This file must be readable
  to the postgres user.

Allowed options
---------------

``delimiter`` (character)
  The CSV delimiter (defaults to  ``,``).

``quotechar`` (character)
  The CSV quote character (defaults to ``"``).

``skip_header`` (integer)
  The number of lines to skip (defaults to ``0``).

Usage example
-------------

Supposing you want to parse the following CSV file, located in ``/tmp/test.csv``::

    Year,Make,Model,Length
    1997,Ford,E350,2.34
    2000,Mercury,Cougar,2.38

You can declare the following table:

.. code-block:: sql
   
    CREATE SERVER csv_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.csvfdw.CsvFdw'
    );
   

    create foreign table csvtest (
           year numeric,
           make character varying,
           model character varying,
           length numeric
    ) server csv_srv options (
           filename '/tmp/test.csv',
           skip_header '1',
           delimiter ',');

    select * from csvtest;

.. code-block:: bash

     year |  make   | model  | length 
    ------+---------+--------+--------
     1997 | Ford    | E350   |   2.34
     2000 | Mercury | Cougar |   2.38
    (2 lines)



FileSystem Foreign Data Wrapper
===============================

Class: ``multicorn.fsfdw.FilesystemFdw``

Source code: `multicorn/fsfdw/__init__.py`_

.. _multicorn/fsfdw/__init__.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/fsfdw/__init__.py

Purpose
-------

This fdw can be used to access data stored in various files, in a filesystem.
The files are looked up based on a pattern, and parts of the file's path are
mapped to various columns, as well as the file's content itself.

Dependencies
------------

No dependency outside the standard python distribution.


Required options
----------------

``root_dir`` (string)
  The base directory from which the pattern is evaluated. The files in this
  directory should be readable by the PostgreSQL user. Ex: ``/var/www/``.

``pattern`` (string)
  A pattern defining which files to match, and wich parts of the file path are
  used as columns. A column name between braces defines a mapping from a path
  part to a column. Ex: ``{artist}/{album}/{trackno} - {trackname}.ogg``.

Allowed options
---------------

``content_column`` (string)
  If set, defines which column will contain the actual file content.

``filename_column`` (string)
  If set, defines which column will contain the full filename.

Usage Example
-------------

Supposing you want to access files in a directory structured like this::

    base_dir/
        artist1/
            album1/
                01 - title1.ogg
                02 - title2.ogg
            album2/
                01 - title1.ogg
                02 - title2.ogg
        artist2/
            album1/
                01 - title1.ogg
                02 - title2.ogg
            album2/
                01 - title1.ogg
                02 - title2.ogg

You can access those files using a foreign table like this:

.. code-block:: sql

    CREATE SERVER filesystem_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.fsfdw.FilesystemFdw'
    );


    CREATE FOREIGN TABLE musicfilesystem (
        artist  character varying,
        album   character varying,
        track   integer,
        title   character varying,
        content bytea,
        filename character varying
    ) server filesystem_srv options(
        root_dir    'base_dir',
        pattern     '{artist}/{album}/{track} - {title}.ogg',
        content_column 'content',
        filename_column 'filename')

Example:

.. code-block:: sql

    SELECT count(track), artist, album from musicfilesystem group by artist, album;

::

     count | artist  | album
    -------+---------+--------
         2 | artist1 | album2
         2 | artist1 | album1
         2 | artist2 | album2
         2 | artist2 | album1
    (4 lines)

SQLite Foreign Data Wrapper
===========================

The sqlite foreign data wrapper has been removed in favor of the more general
sqlalchemy foreign data wrapper.

Imap Foreign Data Wrapper
=========================

Class: ``multicorn.imapfdw.ImapFdw``

Source code: `multicorn/imapfdw.py`

.. _multicorn/imapfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/imapfdw.py

Purpose
-------

This fdw can be used to access mails from an IMAP mailbox.
Column names are mapped to IMAP headers, and two special columns may conain the
mail payload and its flags.

Dependencies
-------------

imaplib

Required options
----------------

``host`` (string)
  The IMAP host to connect to.

``port``
  The IMAP host port to connect to.

``login``
  The login to connect with.

``password``
  The password to connect with.


The login and password options should be set as a user mapping options, so as
not to be stored in plaintext. See `the create user mapping documentation`_

.. _the create user mapping documentation: http://www.postgresql.org/docs/9.1/static/sql-createusermapping.html

Allowed options
---------------

``payload_column`` (string)
  The name of the column which will store the payload.

``flags_column`` (string)
  The name of the column which will store the IMAP flags, as an array of
  strings.

``ssl`` (boolean)
  Wether to use ssl or not

``imap_server_charset`` (string)
  The name of the charset used for IMAP search commands. Defaults to UTF8. For
  the cyrus IMAP server, it should be set to "utf-8".

Server side filtering
---------------------

The imap fdw tries its best to convert postgresql quals into imap filters.

The following quals are pushed to the server:
    - equal, not equal, like, not like comparison
    - = ANY, = NOT ANY

These conditions are matched against the headers, or the body itself.

The imap FDW will fetch only what is needed by the query: you should thus avoid
requesting the payload_column if you don't need it.


RSS Foreign Data Wrapper
========================

Class: ``multicorn.rssfdw.RssFdw``

Source code: `multicorn/rssfdw.py`_

.. _multicorn/rssfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/rssfdw.py

Purpose
-------

This fdw can be used to access items from an rss feed.
The column names are mapped to the elements inside an item.
An rss item has the following strcture:

.. code-block:: xml

    <item>
      <title>Title</title>
      <pubDate>2011-01-02</pubDate>
      <link>http://example.com/test</link>
      <guid>http://example.com/test</link>
      <description>Small description</description>
    </item>

You can access every element by defining a column with the same name. Be
careful to match the case! Example: pubDate should be quoted like this:
``pubDate`` to preserve the uppercased ``D``.


Dependencies
------------

You will need the `lxml`_ library.

.. _lxml: http://lxml.de/

Required options
-----------------

``url`` (string)
  The RSS feed URL.

Usage Example
-------------

If you want to parse the `radicale`_ rss feed, you can use the following
definition:

.. code-block:: sql

    CREATE SERVER rss_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.rssfdw.RssFdw'
    );
    
    CREATE FOREIGN TABLE radicalerss (
        "pubDate" timestamp,
        description character varying,
        title character varying,
        link character varying
    ) server rss_srv options (
        url     'http://radicale.org/rss/'
    );

    select "pubDate", title, link from radicalerss limit 10;

.. code-block:: bash

           pubDate       |              title               |                     link                     
    ---------------------+----------------------------------+----------------------------------------------
     2011-09-27 06:07:42 | Radicale 0.6.2                   | http://radicale.org/news#2011-09-27@06:07:42
     2011-08-28 13:20:46 | Radicale 0.6.1, Changes, Future  | http://radicale.org/news#2011-08-28@13:20:46
     2011-08-01 08:54:43 | Radicale 0.6 Released            | http://radicale.org/news#2011-08-01@08:54:43
     2011-07-02 20:13:29 | Feature Freeze for 0.6           | http://radicale.org/news#2011-07-02@20:13:29
     2011-05-01 17:24:33 | Ready for WSGI                   | http://radicale.org/news#2011-05-01@17:24:33
     2011-04-30 10:21:12 | Apple iCal Support               | http://radicale.org/news#2011-04-30@10:21:12
     2011-04-25 22:10:59 | Two Features and One New Roadmap | http://radicale.org/news#2011-04-25@22:10:59
     2011-04-10 20:04:33 | New Features                     | http://radicale.org/news#2011-04-10@20:04:33
     2011-04-02 12:11:37 | Radicale 0.5 Released            | http://radicale.org/news#2011-04-02@12:11:37
     2011-02-03 23:35:55 | Jabber Room and iPhone Support   | http://radicale.org/news#2011-02-03@23:35:55
    (10 lignes)

LDAP Foreign Data Wrapper
=========================

Class: ``multicorn.ldapfdw.LdapFdw``

Source code: `multicorn/ldapfdw.py`_

.. _multicorn/ldapfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/ldapfdw.py

Purpose
-------

This fdw can be used to access directory servers via the LDAP protocol.
Tested with OpenLDAP.
It supports: simple bind, multiple scopes (subtree, base, etc)

Dependencies
------------

If using Multicorn >= 1.1.0, you will need the `ldap3`_ library:

.. _ldap3: http://pythonhosted.org/python3-ldap/

For prior version, you will need the `ldap`_ library:

.. _ldap: http://www.python-ldap.org/

Required options
----------------

``uri`` (string)
The URI for the server, for example "ldap://localhost".

``path``  (string)
The base in which the search is performed, for example "dc=example,dc=com".

``objectclass`` (string)
The objectClass for which is searched, for example "inetOrgPerson".

``scope`` (string)
The scope: one, sub or base.

Optional options
----------------

``binddn`` (string)
The binddn for example 'cn=admin,dc=example,dc=com'.

``bindpwd`` (string)
The credentials for the binddn.

Usage Example
-------------

To search for a person
definition:

.. code-block:: sql

    CREATE SERVER ldap_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.ldapfdw.LdapFdw'
    );
    
    CREATE FOREIGN TABLE ldapexample (
      	mail character varying,
	cn character varying,
	description character varying
    ) server ldap_srv options (
	uri 'ldap://localhost',
	path 'dc=lab,dc=example,dc=com',
	scope 'sub',
	binddn 'cn=Admin,dc=example,dc=com',
	bindpwd 'admin',
	objectClass '*'
    );

    select * from ldapexample;

.. code-block:: bash

             mail          |        cn      |    description     
    -----------------------+----------------+--------------------
     test@example.com      | test           | 
     admin@example.com     | admin          | LDAP administrator
     someuser@example.com  | Some Test User | 
    (3 rows)

.. _radicale: http://radicale.org/
