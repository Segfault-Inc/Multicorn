.. contents::

Presentation
============

Multicorn is a PostgreSQL extension which goal is to make `Foreign Data Wrapper`_
development easy, by allowing the programmer to use the Python programming
language.

Ok, but why should I care ?
---------------------------

- Multicorn allows you to access any data source in your PostgreSQL database.
- You can leverage the full power of SQL to query your data sources
- Every tool you use for SQL can be reused with those datasources (think about
  an ORM, BI tool...)


Installation
============

With the `pgxn client`_::

    pgxn install multicorn

From pgxn::

    wget http://pgxn.org/dist/multicorn/0.0.2/ ./
    unzip multicorn-0.0.2.zip
    cd multicorn-0.0.2/
    make && sudo make install

From source::

    git clone git://github.com/Kozea/Multicorn.git
    cd Multicorn
    make && make install

.. _Foreign Data Wrapper: http://people.planetpostgresql.org/andrew/uploads/fdw2.pdf
.. _pgxn client: http://pgxnclient.projects.postgresql.org/


Usage
=====

The multicorn foreign data wrapper is not different from other foreign data
wrappers.

To use it, you have to:

- Create the extension in the target database.
  As a PostgreSQL super user, run the following SQL:

  .. code-block:: sql

      CREATE EXTENSION multicorn;

- Create a "foreign server":

  .. code-block:: sql

      CREATE SERVER multicorn_srv FOREIGN DATA WRAPPER multicorn;

You can then proceed on with the actual foreign tables creation.

In the SQL ``OPTIONS`` clause, you must provide an options named wrapper,
containing the fully-qualified class name of the concrete python foreign data
wrapper you wish to use.

For example, if you want to use the google foreign data wrapper, you can define
it like this:

.. code-block:: sql

    create foreign table googletest (
           url character varying,
           title character varying,
           "search" character varying
    ) server multicorn_srv options (
           wrapper 'multicorn.googlefdw.GoogleFdw');

    select url, link from googletest where search = 'Multicorn';

.. code-block:: bash


                          url                       |        link         
    ------------------------------------------------+-------------------------
     http://wiki.answers.com/Q/What_is_a_multi-corn | What is a multi-corn...
     http://www.myspace.com/multicorn               | Multicorn | Free Mus...
     http://multicorn.org/                          | Multicorn - Unified ...
     http://www.reddit.com/user/Multicorn           | overview for Multico...
    (4 lignes)





Each foreign data wrapper supports its own set of options, and may interpret the
columns definitions differently.

Please look at the documentation for the available `foreign data wrappers`_.

.. _foreign data wrappers: /foreign-data-wrappers/

.. XXX disabled until the page is created
.. If you want to write your own foreign data wrapper, go read the `implementor's
.. guide`_.

.. _implementor's guide: /implementing-a-fdw/
