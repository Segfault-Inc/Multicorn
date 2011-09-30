.. contents::

Writing an FDW
==============

Multicorn provides a simple interface for writing foreign data wrappers: the
``multicorn.ForeignDataWrapper`` interface.

Implementing a foreign data wrapper is as simple as inheriting from ``multicorn.ForeignDataWrapper`` and implemening the ``execute`` method.

What are we trying to achieve ?
===============================

Supposing we want to implement a foreign data wrapper which only returns a set
of 20 rows, containing in each column the name of the column itself concatenated
with the number of the line.

The goal of this tutorial is to be able to execute this:

.. code-block:: sql

    CREATE FOREIGN TABLE constanttable (
        test character varying,
        test2 character varying
    ) server multicorn_srv options (
        wrapper 'myfdw.ConstantForeignDataWrapper'
    )

    SELECT * from constanttable;

And obtain this as a result:

.. code-block:: bash

      test   |  test2   
    ---------+----------
     test 0  | test2 0
     test 1  | test2 1
     test 2  | test2 2
     test 3  | test2 3
     test 4  | test2 4
     test 5  | test2 5
     test 6  | test2 6
     test 7  | test2 7
     test 8  | test2 8
     test 9  | test2 9
     test 10 | test2 10
     test 11 | test2 11
     test 12 | test2 12
     test 13 | test2 13
     test 14 | test2 14
     test 15 | test2 15
     test 16 | test2 16
     test 17 | test2 17
     test 18 | test2 18
     test 19 | test2 19
    (20 lignes)

How do we do that ?
===================

The fdw described above is pretty simple, implementing it should be easy !

But let's see the whole code. To be usable with the above ``CREATE FOREIGN
TABLE`` statement, this module should be named ``myfdw.py`` AND installed in the
system-wide python distribution.

.. code-block:: python

    from multicorn import ForeignDataWrapper

    # The class must extend ForeignDataWrapper, or at least conform to its
    # interface
    class ConstantForeignDataWrapper(ForeignDataWrapper):
        
        def __init__(self, options, columns):
            super(ConstantForeignDataWrapper, self).__init__(options, columns)
            self.columns = columns

        def execute(self, quals):
            for index in range(20):
                # Here, we chose to build a dictionary.
                # Each column contains the concatenation of the column name and
                # the line index.
                line = {}
                for column_name in self.columns:
                    line[column_name] = '%s %s' % (column_name, index)
                yield line


And that's it !
You just created your first foreign data wrapper. But let's look a bit more
thoroughly to the class...

The first thing to do (although optional, since you can implement the interface
via duck-typing), is to import the base class and subclass it:

.. code-block:: python

    from multicorn import ForeignDataWrapper

    class ConstantForeignDataWrapper(ForeignDataWrapper):

The init method must accept two arguments

``options``
    A dictionary of options given in the ``OPTIONS`` clause of the 
    ``CREATE FOREIGN TABLE`` statement, minus the wrapper option.

``columns``
    A list of the columns names given during the table creation.

Our access point do not need any options, thus we will only need to keep a
reference to the columns:

.. code-block:: python
   
    def __init__(self, options, columns):
        super(ConstantForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns


The execute method is the core of the API.
It is called with a list of ``Qual`` objects, which we will ignore 
for now but more on that `later <#optimizations>`_.

This method must return an iterable of the resulting lines.
Each line can be either a list containing an item by column,
or a dictonary mappning the column names to their value.

For this example, we chose to build a dictionary.
Each column contains the concatenation of the column name and
the line index.

.. code-block:: python

        def execute(self, quals):
            for index in range(20):
                line = {}
                for column_name in self.columns:
                    line[column_name] = '%s %s' % (column_name, index)
                yield line


And that's it !


Optimizations
=============

As was noted in the code commentaries, the execute methods accept a ``quals`` argument.
This argument is a list of quals object, which are defined in `multicorn/__init__.py`_.
A Qual object defines a simple condition wich can be used by the foreign data
wrapper to restrict the number of the results.
The Qual class defines three instance's attributes:

- field_name: the name of the column concerned by the condition.
- operator: the name of the operator.
- value: the value expressed in the condition.

Let's suppose we write the following query:

.. code-block:: sql

    SELECT * from constanttable where test = 'test 2' and test2 like '%3%';

The method execute would be called with the following quals:

.. code-block:: python 
    
    [Qual('test', '=', 'test 2'), Qual('test', '~~', '3')]

Now you can use this information to reduce the set of results to return to the
postgresql server.

.. note:: 

    You don't HAVE to enforce those quals, Postgresql will check them anyway.
    It's nonetheless useful to reduce the amount of results you fetch over the
    network, for example.


.. _multicorn/__init__.py: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/__init__.py

Error reporting
===============

In the `multicorn.utils`_ module lies a simple utility function,
``log_to_postgres``.


.. _multicorn.utils: https://github.com/Kozea/Multicorn/blob/master/python/multicorn/utils.py

This function is mapped to the Postgresql function erreport.

It accepts three arguments:

``message`` (required)
    A python string containing the message to report.

``level`` (optional, defaults to ``logging.INFO``)
    The severity of the message. The following values are accepted:
        ``logging.DEBUG``
            Maps to a postgresql DEBUG1 message. In most configurations, it won't
            show at all.
        ``logging.INFO``
            Maps to a postgresql NOTICE message. A NOTICE message is passed to the
            client, as well as in the server logs.
        ``logging.WARNING``
            Maps to a postgresql WARNING message. A WARNING message is passed to the
            client, as well as in the server logs.
        ``logging.ERROR``
            Maps to a postgresql ERROR message. An ERROR message is passed to the
            client, as well as in the server logs.
            
            .. important:: 
                
                An ERROR message results in the current transaction being aborted.
                Think about the consequences when you uses it !

        ``logging.CRITICAL``
            Maps to a postgresql FATAL message. Causes the current server process
            to abort.

            .. important:: 
                
                A CRITICAL message results in the current server process to be aborted
                Think about the consequences when you uses it !

``hint`` (optional)
    An hint given to the user to resolve the cause of the message (ex:``Try
    adding the missing option in the table creation statement``) 


Foreign Data Wrapper lifecycle
==============================

The foreign data wrapper associated to a table is instantiated on a per-process
basis, and it happens when the first query is run against it.

Usually, postgresql server processes are spawned on a per-connection basis.

During the life time of a server process, the instance is cached.
That means that if you have to keep references to resources such as connections,
you should establish them in the ``__init__`` method and cache them as instance
attributes.
