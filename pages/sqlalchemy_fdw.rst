SQLAlchemy_fdw
==============

`Sqlalchemy_fdw`_ is a dialect for sqlalchemy bringing foreign tables creation and
managemenet to sqlalchemy.

.. _Sqlalchemy_fdw: https://github.com/Kozea/sqlalchemy_fdw

.. note::

    If you only want to access the data in your foreign tables with sqlalchemy,
    and not manage their creation, destruction from within the framework, you
    don't need sqlalchemy_fdw: a simple, standard sqlalchemy Table is more than
    sufficient.


Installation
============

From source:

.. code-block:: bash

    git clone git://github.com/Kozea/sqlalchemy_fdw.git
    cd sqlalchemy_fdw
    python ./setup.py install

Usage
=====


Sqlalchemy_fdw is a dialect for sqlalchemy, based on the psycopg2 dialect.

The connection string for sqlalchemy_fdw is pgddw. 

Example:

.. code-block:: python

    engine = create_engine('pgfdw://user:password@host:port/dbname')


Two new classes are available for managing schema constructs

- `ForeignTable`_
- `ForeignDataWrapper`_

ForeignDataWrapper
==================

This class represents a postgresql Foreign server.

You can instantiate it like this:

.. code-block:: python

    fdw = ForeignDataWrapper("myfdwserver", "myfdwextension", metadata=metadata,
                            options={'option1': 'test'})

And then order its creation with:

.. code-block:: python

    fdw.create(checkfirst=True)


This will result in the following sql statement:

.. code-block:: sql

    CREATE SERVER myfdwserver FOREIGN DATA WRAPPER myfdwextension
    OPTIONS (
        "option1" 'test'
    )

You can also drop it like this:

.. code-block:: python

    fdw.drop(checkfirst=True, cascade=True)

The constructor accepts the following arguments:

``name`` (mandatory)
    The foreign server name (ex: multicorn_srv).

``extension_name`` (mandatory)
    The foreign data wrapper extension name (ex: multicorn).

``metadata`` (optional)
    The ``Metadata`` to use to create a binding.

``bind`` (optional)
    An ``Engine`` object to bind to

``options`` (optional)
    A dictonary to be usesd as an OPTIONS clause

ForeignTable
============

The foreign table class represents a Foreign Table schema object.
The foreign table inherits from the good old ``Table`` class from alchemy.
Everything is available as expected.

The foreign table class takes two more arguments than the standard table: 


``fdw_server`` (mandatory)
    The name of the fdw server used in the ``CREATE FOREIGN TABLE`` statement

``fdw_options`` (optional)
    A dictionary containing options used in the ``CREATE FOREIGN TABLE``
    statement's ``OPTION`` clause.

Example:

.. code-block:: python

    table = ForeignTable("myforeigntable", metadata,
                Column('col1', Integer),
                Column('col2', Unicode),
                fdw_server='myfdwserver',
                fdw_options={ 
                    'tableoption': 'optionvalue'
                }
            )
    table.create(checkfirst=True)

Will result in the following statement:

.. code-block:: sql

    CREATE FOREIGN TABLE myforeigntable (
        col1 integer,
        col2 character varying
    ) server myfdwserver options (
        tableoption 'optionvalue
    );
