***
API
***

The API is split into two modules: the ``multicorn`` module and the
`utils` module:

  - The ``multicorn`` module contains the whole API needed for implementing
    a Foreign Data Wrapper.
  - The ``utils`` module contains logging and error reporting functions,
    which are ultimately implemented as calls to the PostgreSQL API.


Implementing an FDW
===================

Implementing an FDW is as simple as implementing the
:py:class:`~multicorn.ForeignDataWrapper` class.



Required API
------------

.. py:currentmodule:: multicorn.ForeignDataWrapper

This subset of the API allows your ForeignDataWrapper to be used for read-only
queries.

You have to implement the following methods:

  - :py:meth:`__init__`
  - :py:meth:`execute`

.. note:: In the documentation, FDWs implementing this API will be marked with:

          .. api_compat:: :read:


Write API
---------

To implement full write capabilites, the following property must be implemented:


.. autoattribute:: multicorn.ForeignDataWrapper.rowid_column

In addition to that, you should implement each DML operation as you see fit:

  - :py:meth:`insert`
  - :py:meth:`update`
  - :py:meth:`delete`



.. note:: In the documentation, FDWs implementing this API will be marked with:

          .. api_compat::
            :write:

Transactional API
-----------------

Transactional Capabilities can be implemented with the following methods:


.. automethod:: multicorn.ForeignDataWrapper.begin
.. automethod:: multicorn.ForeignDataWrapper.pre_commit
.. automethod:: multicorn.ForeignDataWrapper.rollback
.. automethod:: multicorn.ForeignDataWrapper.sub_begin
.. automethod:: multicorn.ForeignDataWrapper.sub_commit
.. automethod:: multicorn.ForeignDataWrapper.sub_rollback

.. note:: In the documentation, FDWs implementing this API will be marked with:

          .. api_compat::
            :transaction:


Full API
========

.. autoclass:: multicorn.ForeignDataWrapper
   :special-members: __init__
   :members:


.. autoclass:: multicorn.SortKey

.. autoclass:: multicorn.Qual
   :members:

.. autoclass:: multicorn.ColumnDefinition
   :members:

.. autoclass:: multicorn.TableDefinition
   :members:


