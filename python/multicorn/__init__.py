"""
Base mulricorn module.

This module contains all the python code needed by the multicorn C extension
to postgresql.

You should install it in the python path available to the user running
postgresql (usually, the system wide python installation).

"""

from importlib import import_module


class Qual(object):
    """A Qual describes a postgresql qual.

    Attributes
    field_name      -- The name of the column as defined in the postgresql
                       table.
    operator_name   -- The name of the operator.
                       Example: =, <=, ~~ (for a like clause)


    """

    def __init__(self, field_name, operator, value):
        """Constructs a qual object.

        Instantiated from the C extension with the field name, operator and
        value extracted from the postgresql where clause.

        """
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def __repr__(self):
        return "%s %s %s" % (self.field_name, self.operator, self.value)


class ForeignDataWrapper(object):
    """Base class for all foreign data wrapper instances."""

    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.

        Arguments
        fdw_options -- The foreign data wrapper options. It is a dictionary
                       mapping keys from the sql "CREATE FOREIGN TABLE"
                       statement options. It is left to the implementor
                       to decide what should be put in those options, and what
                       to do with them.
        fdw_columns -- The foreign datawrapper columns. It is a sequence
                       containing the column names.

        """
        pass

    def execute(self, quals):
        """Execute a query in the foreign data wrapper.

        Arguments
        quals       -- A list of :class:`Qual` instances, containing the basic
                       where clauses in the query.
                       Implementors are not expected to manage these quals,
                       since postgresql will check them anyway.
                       For an exemple of quals management, see the concrete
                       subclass :class:`multicorn.ldapfdw.LdapFdw`


        """
        pass


def get_class(module_path):
    """Internal function called from c code to import a foreign data wrapper.

    Returns the class designated by module_path.

    Arguments
    module_path     -- A fully qualified path to a foreign data wrapper.
                       Ex: multicorn.csvfdw.CsvFdw.

    """
    module_path.split(".")
    wrapper_class = module_path.split(".")[-1]
    module_name = ".".join(module_path.split(".")[:-1])
    module = import_module(module_name)
    return getattr(module, wrapper_class)
