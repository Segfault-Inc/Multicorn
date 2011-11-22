"""
Base mulricorn module.

This module contains all the python code needed by the multicorn C extension
to postgresql.

You should install it in the python path available to the user running
postgresql (usually, the system wide python installation).

"""

from importlib import import_module


ANY = object()
ALL = object()

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

    @property
    def is_list_operator(self):
        """Returns True if this qual represents an array expr"""
        return isinstance(self.operator, tuple)

    @property
    def list_any_or_all(self):
        """Returns ANY if and only if:
            - this is a list operator
            - the operator applies as an 'ANY' clause (eg, = ANY(1,2,3))
           Returns ALL if and only if:
            - this is a list operator
            - the operator applies as an 'ALL' clause (eg, > ALL(1, 2, 3))
           Else, returns None     
        """
        if self.is_list_operator:
            return ANY if self.operator[1] else ALL
        return None

    def __repr__(self):
        if self.is_list_operator:
            value = '%s(%s)' % ('ANY' if self.list_any_or_all == ANY else 'ALL',
                    self.value)
            operator = self.operator[0]
        else:
            value = self.value
            operator = self.operator
        return (u"%s %s %s" % (self.field_name, operator, value))\
                .encode('utf8')


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

    def execute(self, quals, columns):
        """Execute a query in the foreign data wrapper.

        Arguments
        quals       -- A list of :class:`Qual` instances, containing the basic
                       where clauses in the query.
                       Implementors are not expected to manage these quals,
                       since postgresql will check them anyway.
                       For an exemple of quals management, see the concrete
                       subclass :class:`multicorn.ldapfdw.LdapFdw`

        columns     -- A list of columns that postgresql is going to need.
                    You should return AT LEAST those columns when returning a
                    dict. If returning a sequence, every column from the table
                    should be in
                    the sequence.

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
