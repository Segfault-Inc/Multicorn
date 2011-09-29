.. contents::

Foreign Data Wrappers
=====================

Multicorn is bundled with a small set of Foreign Data Wrappers, which you can
use or customize for your needs.

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

    create foreign table csvtest (
           year numeric,
           make character varying,
           model character varying,
           length numeric
    ) server multicorn_srv options (
           wrapper 'multicorn.csvfdw.CsvFdw',
           filename '/tmp/test.csv',
           skip_header '1',
           delimiter ',');


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

    CREATE FOREIGN TABLE musicfilesystem (
        artist  character varying,
        album   character varying,
        track   integer,
        title   character varying,
        content bytea,
        filename character varying
    ) server multicorn_srv options(
        wrapper     'multicorn.fsfdw.FilesystemFdw',
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

Class: ``multicorn.sqlitefdw.SqliteFdw``

Source code: `multicorn/sqlitefdw.py`_

.. _multicorn/sqlitefdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/sqlitefdw.py

Purpose
-------

This fdw can be used to access data stored in tables in a sqlite database.

Dependencies
------------

No dependency outside the standard python distribution.

Required options
----------------

``database`` (string)
  The sqlite database to connect to. Examples: ``/tmp/mydatabase.db``,
  ``:memory:``

``tablename`` (string)
  The name of the mapped table.

Usage Example
-------------

Let's suppose you want to access an sqlite3 database located at ``/tmp/data.db``.

.. code-block:: sql

    CREATE FOREIGN TABLE sqlitetest (
        column1 integer,
        column2 character varying
        ...etc..
    ) server multicorn_srv options (
        wrapper     'multicorn.sqlitefdw.SqliteFdw',
        database    '/tmp/data.csv',
        tablename   'table1'
    )



RSS Foreign Data Wrapper
========================

Class: ``multicorn.rssfdw.RssFdw``

Source code: `multicorn/rssfdw.py`_

.. _multicorn/rssfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/rssfdw.py

Purpose
-------

This fdw can be used tgo access items from an rss feed.
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

Required options
-----------------

``url`` (string)
  The RSS feed URL.

Usage Example
-------------

If you want to parse the `radicale`_ rss feed, you can use the following
definition:

.. code-block:: sql

    CREATE FOREIGN TABLE radicalerss (
        "pubDate" timestamp,
        description character varying,
        link character varying
    ) server multicorn_srv options (
        wrapper 'multicorn.rssfdw.RssFdw'
    );

.. _radicale: http://radicale.org/

GIT Foreign Data Wrapper
========================

Class: ``multicorn.gitfdw.GitFdw``

Source code: `multicorn/gitfdw.py`_

.. _multicorn/gitfdw.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/gitfdw.py

Purpose
-------

This fdw can be used
