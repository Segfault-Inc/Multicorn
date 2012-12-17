"""A SQLAlchemy foreign data wrapper"""

from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR, WARNING, DEBUG
from sqlalchemy import create_engine
from sqlalchemy.sql import select, operators as sqlops, and_
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.dialects.postgresql.base import ischema_names
import operator


def compose(*funs):
    if len(funs) == 0:
        raise ValueError("At least one function is necessary for compose")
    if len(funs) == 1:
        return funs[0]
    else:
        result_fun = compose(*funs[1:])
        return lambda *args, **kwargs: funs[0](result_fun(*args, **kwargs))


def not_(function):
    return compose(operator.inv, function)


OPERATORS = {
    '=': operator.eq,
    '<': operator.lt,
    '>': operator.gt,
    '<=': operator.le,
    '>=': operator.ge,
    '<>': operator.ne,
    '~~': sqlops.like_op,
    '~~*': sqlops.ilike_op,
    '!~~*': not_(sqlops.ilike_op),
    '!~~': not_(sqlops.like_op),
    ('=', True): sqlops.in_op,
    ('<>', False): not_(sqlops.in_op)
}


class SqlAlchemyFdw(ForeignDataWrapper):
    """An SqlAlchemy foreign data wrapper.

    The sqlalchemy foreign data wrapper performs simple selects on a remote
    database using the sqlalchemy framework.

    Accepted options:

    db_url      --  the sqlalchemy connection string.
    tablename   --  the table name in the remote database.

    """

    def __init__(self, fdw_options, fdw_columns):
        super(SqlAlchemyFdw, self).__init__(fdw_options, fdw_columns)
        if 'db_url' not in fdw_options:
            log_to_postgres('The db_url parameter is required', ERROR)
        if 'tablename' not in fdw_options:
            log_to_postgres('The tablename parameter is required', ERROR)
        self.engine = create_engine(fdw_options.get('db_url'))
        self.metadata = MetaData()
        tablename = fdw_options['tablename']
        self.table = Table(tablename, self.metadata,
                           *[Column(col.column_name, ischema_names[col.type_name])
                             for col in fdw_columns.values()])

    def execute(self, quals, columns):
        """
        The quals are turned into an and'ed where clause.
        """
        statement = select([self.table])
        clauses = []
        for qual in quals:
            operator = OPERATORS.get(qual.operator, None)
            if operator:
                clauses.append(operator(self.table.c[qual.field_name],
                                        qual.value))
            else:
                log_to_postgres('Qual not pushed to foreign db: %s' % qual,
                                WARNING)
        if clauses:
            statement = statement.where(and_(*clauses))
        columns = [self.table.c[col] for col in columns]
        statement = statement.with_only_columns(columns)
        log_to_postgres(unicode(statement), DEBUG)
        for item in self.engine.execute(statement):
            yield item
