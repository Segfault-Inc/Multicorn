# -*- coding: utf-8 -*-
"""
Base multicorn module.

This module contains all the python code needed by the multicorn C extension
to postgresql.

You should install it in the python path available to the user running
postgresql (usually, the system wide python installation).

"""

import sys

__version__ = '__VERSION__'

ANY = object()
ALL = object()
UNBOUND = object()


class Qual(object):
    """A Qual describes a postgresql qualifier.

    A qualifier is here defined as an expression of the type::

        col_name operator value

    For example::

        mycolumn > 3
        mycolumn = ANY(1,2,3)
        mycolumn ~~ ALL('A%','AB%', '%C')



    Attributes:
        field_name (str):    The name of the column as defined in the postgresql
            table.
        operator (str or tuple): The name of the operator if a string.
            Example: =, <=, ~~ (for a like clause)

            If it is a tuple, then the tuple is of the form (operator name, ANY or ALL).

            The tuple represents a comparison of the form WHERE field = ANY(1, 2, 3), which
            is the internal representation of WHERE field IN (1, 2, 3)
        value (object): The constant value on the right side


    """

    def __init__(self, field_name, operator, value):
        """Constructs a qual object.

        Instantiated from the C extension with the field name, operator and
        value extracted from the postgresql where clause.

        Accepts every field from the qual.
        """
        self.field_name = field_name
        self.operator = operator
        self.value = value

    @property
    def is_list_operator(self):
        """
        Returns:
            True if this qual represents an array expr, False otherwise
        """
        return isinstance(self.operator, tuple)

    @property
    def list_any_or_all(self):
        """
        Returns:
            ANY if and only if:
                - this qual is a list operator
                - the operator applies as an 'ANY' clause (eg, = ANY(1,2,3))

            ALL if and only if:
                - this is a list operator
                - the operator applies as an 'ALL' clause (eg, > ALL(1, 2, 3))

            None if this is not a list operator.

        """
        if self.is_list_operator:
            return ANY if self.operator[1] else ALL
        return None

    def __repr__(self):
        if self.is_list_operator:
            value = '%s(%s)' % (
                'ANY' if self.list_any_or_all == ANY else 'ALL',
                self.value)
            operator = self.operator[0]
        else:
            value = self.value
            operator = self.operator
        return ("%s %s %s" % (self.field_name, operator, value))

    def __eq__(self, other):
        if isinstance(other, Qual):
            return (self.field_name == other.field_name and
                    self.operator == other.operator and
                    self.value == other.value)
        return False

    def __hash__(self):
        return hash((self.field_name, self.operator, self.value))


class ForeignDataWrapper(object):
    """Base class for all foreign data wrapper instances.

    Though not required, ForeignDataWrapper implementation should
    inherit from this class.
    """

    _startup_cost = 20

    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.

        Args:
            fdw_options (dict): The foreign data wrapper options. It is a dictionary
                mapping keys from the sql "CREATE FOREIGN TABLE"
                statement options. It is left to the implementor
                to decide what should be put in those options, and what
                to do with them.
            fdw_columns (dict): The foreign datawrapper columns. It is a dictionary
                       mapping the column names to their ColumnDefinition.

        """
        pass

    def get_rel_size(self, quals, columns):
        """
        Method called from the planner to estimate the resulting relation
        size for a scan.

        It will help the planner in deciding between different types of plans,
        according to their costs.

        Args:
            quals (list): A list of Qual instances describing the filters
                applied to this scan.
            columns (list): The list of columns that must be returned.

        Returns:
            A tuple of the form (expected_number_of_rows, avg_row_width (in bytes))
        """
        return (100000000, len(columns) * 100)

    def get_path_keys(self):
        u"""
        Method called from the planner to add additional Path to the planner.
        By default, the planner generates an (unparameterized) path, which
        can be reasoned about like a SequentialScan, optionally filtered.

        This method allows the implementor to declare other Paths,
        corresponding to faster access methods for specific attributes.
        Such a parameterized path can be reasoned about like an IndexScan.


        For example, with the following query::

            select * from foreign_table inner join local_table using(id);


        where foreign_table is a foreign table containing 100000 rows, and
        local_table is a regular table containing 100 rows.

        The previous query would probably be transformed to a plan similar to
        this one::

            ┌────────────────────────────────────────────────────────────────────────────────────┐
            │                                     QUERY PLAN                                     │
            ├────────────────────────────────────────────────────────────────────────────────────┤
            │ Hash Join  (cost=57.67..4021812.67 rows=615000 width=68)                           │
            │   Hash Cond: (foreign_table.id = local_table.id)                                   │
            │   ->  Foreign Scan on foreign_table (cost=20.00..4000000.00 rows=100000 width=40)  │
            │   ->  Hash  (cost=22.30..22.30 rows=1230 width=36)                                 │
            │         ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)          │
            └────────────────────────────────────────────────────────────────────────────────────┘

        But with a parameterized path declared on the id key, with the knowledge that this key
        is unique on the foreign side, the following plan might get chosen::

            ┌───────────────────────────────────────────────────────────────────────┐
            │                              QUERY PLAN                               │
            ├───────────────────────────────────────────────────────────────────────┤
            │ Nested Loop  (cost=20.00..49234.60 rows=615000 width=68)              │
            │   ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)   │
            │   ->  Foreign Scan on remote_table (cost=20.00..40.00 rows=1 width=40)│
            │         Filter: (id = local_table.id)                                 │
            └───────────────────────────────────────────────────────────────────────┘


        Returns:
            A list of tuples of the form: (key_columns, expected_rows),
            where key_columns is a tuple containing the columns on which
            the path can be used, and expected_rows is the number of rows
            this path might return for a simple lookup.
            For example, the return value corresponding to the previous scenario would be::

                [(('id',), 1)]

        """
        return []

    def execute(self, quals, columns):
        """Execute a query in the foreign data wrapper.

        This method is called at the first iteration.
        This is where the actual remote query execution takes place. Multicorn
        makes no assumption about the particular behavior of a
        ForeignDataWrapper, and will NOT remove any qualifiers from the
        PostgreSQL quals list. That means the quals will be rechecked anyway.

        Typically, an implementation would:

            - initialize (or reuse) some sort of connection to the
              remote system
            - transform the quals and columns arguments to a representation
              suitable for the remote system
            - fetch the data according to this query
            - return it to the C-extension.


        Although any iterable can be returned, it is strongly advised to
        implement this method as a generator to prevent loading the whole
        dataset in memory.


        Args:
            quals (list): A list of :class:`Qual` instances, containing the basic
                where clauses in the query.
            columns (list):  A list of columns that postgresql is going to need.
                You should return AT LEAST those columns when returning a
                dict. If returning a sequence, every column from the table
                should be in the sequence.

        Returns:
            An iterable of python objects which can be converted back to PostgreSQL.
            Currently, such objects are:
            - sequences containing exactly as much columns as the
            underlying tables
            - dictionaries mapping column names to their values.

        """
        pass

    @property
    def rowid_column(self):
        """
        Returns:
            A column name which will act as a rowid column,
            for delete/update operations.

            One can think of it as a primary key.

            This can be either an existing column name, or a made-up one.
            This column name should be subsequently present in every
            returned resultset.
        """
        raise NotImplementedError("This FDW does not support the writable API")

    def insert(self, values):
        """
        Insert a tuple defined by ''values'' in the foreign table.

        Args:
            values (dict): a dictionary mapping column names to column values
        Returns:
            A dictionary containing the new values. These values can differ
            from the ``values`` argument if any one of them was changed
            or inserted by the foreign side. For example, if a key is auto
            generated.
        """
        raise NotImplementedError("This FDW does not support the writable API")

    def update(self, oldvalues, newvalues):
        """
        Update a tuple containing ''oldvalues'' to the ''newvalues''.

        Args:
            oldvalues (dict): a dictionary mapping from column
                names to previously known values for the tuple.
            newvalues (dict): a dictionary mapping from column names to new
                values for the tuple.
        Returns:
            A dictionary containing the new values. See :method:``insert``
            for information about this return value.
        """
        raise NotImplementedError("This FDW does not support the writable API")

    def delete(self, oldvalues):
        """
        Delete a tuple identified by ``oldvalues``

        Args:
            oldvalues (dict): a dictionary mapping from column names to
                previously known values for the tuple.
        Returns:
            None
        """
        raise NotImplementedError("This FDW does not support the writable API")

    def pre_commit(self):
        """
        Hook called just before a commit is issued, on PostgreSQL >=9.3.
        This is where the transaction should tentatively commited.
        """
        pass

    def rollback(self):
        """
        Hook called when the transaction is rollbacked.
        """
        pass

    def commit(self):
        """
        Hook called at commit time. On PostgreSQL >= 9.3, the pre_commit
        hook should be preferred.
        """
        pass

    def end_scan(self):
        """
        Hook called at the end of a foreign scan.
        """
        pass

    def end_modify(self):
        """
        Hook called at the end of a foreign modify (DML operations)
        """
        pass

    def begin(self, serializable):
        """
        Hook called at the beginning of a transaction.
        """
        pass

    def sub_begin(self, level):
        """
        Hook called at the beginning of a subtransaction.
        """
        pass

    def sub_rollback(self, level):
        """
        Hook called when a subtransaction is rollbacked.
        """
        pass

    def sub_commit(self, level):
        """
        Hook called when a subtransaction is committed.
        """
        pass


class TransactionAwareForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(TransactionAwareForeignDataWrapper, self).__init__(
            fdw_options, fdw_columns)
        self._init_transaction_state()

    def _init_transaction_state(self):
        self.current_transaction_state = []

    def insert(self, values):
        self.current_transaction_state.append(('insert', values))

    def update(self, oldvalues, newvalues):
        self.current_transaction_state.append(
            ('update', (oldvalues, newvalues)))

    def delete(self, oldvalues):
        self.current_transaction_state.append(('delete', oldvalues))

    def rollback(self):
        self._init_transaction_state()


"""Code from python2.7 importlib.import_module."""
"""Backport of importlib.import_module from 3.x."""
# While not critical (and in no way guaranteed!), it would be nice to keep this
# code compatible with Python 2.3.


def _resolve_name(name, package, level):
    """Return the absolute name of the module to be imported."""
    if not hasattr(package, 'rindex'):
        raise ValueError("'package' not set to a string")
    dot = len(package)
    for x in range(level, 1, -1):
        try:
            dot = package.rindex('.', 0, dot)
        except ValueError:
            raise ValueError("attempted relative import beyond top-level "
                             "package")
    return "%s.%s" % (package[:dot], name)


def import_module(name, package=None):
    """Import a module.


    The 'package' argument is required when performing a relative import. It
    specifies the package to use as the anchor point from which to resolve the
    relative import to an absolute import.

    """
    if name.startswith('.'):
        if not package:
            raise TypeError("relative imports require the 'package' argument")
        level = 0
        for character in name:
            if character != '.':
                break
            level += 1
        name = _resolve_name(name[level:], package, level)
    __import__(name)
    return sys.modules[name]


def get_class(module_path):
    """
    Internal function called from c code to import a foreign data wrapper.


    Args:
        module_path (str): A fully qualified name for a class. to import
                       Ex: multicorn.csvfdw.CsvFdw.


    Returns:
        the class designated by module_path.
    """
    module_path.split(".")
    wrapper_class = module_path.split(".")[-1]
    module_name = ".".join(module_path.split(".")[:-1])
    module = import_module(module_name)
    return getattr(module, wrapper_class)


class ColumnDefinition(object):
    """
    Definition of Foreign Table Column.

    Attributes:
        column_name (str): the name of the column
        type_oid (int): the internal OID of the PostgreSQL type
        typmod (int): the type modifier (ex: VARCHAR(12))
        type_name (str): the formatted type name, with the modifier (ex: VARCHAR(12))
        base_type_name (str): the base type name, withou modifier (ex: VARCHAR)
        options (dict): a mapping of option names to option values, as strings.

    """


    def __init__(self, column_name, type_oid, typmod, type_name,
                 base_type_name,
                 options):
        self.column_name = column_name
        self.type_oid = type_oid
        self.typmod = typmod
        self.type_name = type_name
        self.base_type_name = base_type_name
        self.options = options or {}

    def __repr__(self):
        return "%s(%s, %i, %s%s)" % (
            self.__class__.__name__, self.column_name,
            self.type_oid, self.type_name,
            " options %s" % self.options if self.options else "")
