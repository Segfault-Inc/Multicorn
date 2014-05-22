"""A SQLAlchemy foreign data wrapper"""

from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR, WARNING, DEBUG
from sqlalchemy import create_engine
from sqlalchemy.sql import select, operators as sqlops, and_
# Handle the sqlalchemy 0.8 / 0.9 changes
try:
    from sqlalchemy.sql import sqltypes
except ImportError:
    from sqlalchemy import types as sqltypes

from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.dialects.postgresql.base import ARRAY, ischema_names
import re
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
    schema      --  (optional) schema name to qualify table name with
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
        schema = fdw_options['schema'] if 'schema' in fdw_options else None
        tablename = fdw_options['tablename']
        sqlacols = []
        for col in fdw_columns.values():
            col_type = self._get_column_type(col.type_name)
            sqlacols.append(Column(col.column_name, col_type))
        self.table = Table(tablename, self.metadata, schema=schema,
                           *sqlacols)
        self.transaction = None
        self._connection = None
        self._row_id_column = fdw_options.get('primary_key', None)

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
        if columns:
            columns = [self.table.c[col] for col in columns]
        else:
            columns = self.table.c.values()
        statement = statement.with_only_columns(columns)
        log_to_postgres(str(statement), DEBUG)
        for item in self.connection.execute(statement):
            yield dict(item)

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.engine.connect()
        return self._connection

    def begin(self, serializable):
        self.transaction = self.connection.begin()

    def pre_commit(self):
        if self.transaction is not None:
            self.transaction.commit()
            self.transaction = None

    def commit(self):
        # Pre-commit hook does this on 9.3
        if self.transaction is not None:
            self.transaction.commit()
            self.transaction = None

    def rollback(self):
        if self.transaction is not None:
            self.transaction.rollback()
            self.transaction = None

    @property
    def rowid_column(self):
        if self._row_id_column is None:
            log_to_postgres(
                'You need to declare a primary key option in order '
                'to use the write features')
        return self._row_id_column

    def insert(self, values):
        self.connection.execute(self.table.insert(values=values))

    def update(self, rowid, newvalues):
        self.connection.execute(
            self.table.update()
            .where(self.table.c[self._row_id_column] == rowid)
            .values(newvalues))

    def delete(self, rowid):
        self.connection.execute(
            self.table.delete()
            .where(self.table.c[self._row_id_column] == rowid))

    def _get_column_type(self, format_type):
        """Blatant ripoff from PG_Dialect.get_column_info"""
        ## strip (*) from character varying(5), timestamp(5)
        # with time zone, geometry(POLYGON), etc.
        attype = re.sub(r'\(.*\)', '', format_type)

        # strip '[]' from integer[], etc.
        attype = re.sub(r'\[\]', '', attype)

        is_array = format_type.endswith('[]')
        charlen = re.search('\(([\d,]+)\)', format_type)
        if charlen:
            charlen = charlen.group(1)
        args = re.search('\((.*)\)', format_type)
        if args and args.group(1):
            args = tuple(re.split('\s*,\s*', args.group(1)))
        else:
            args = ()
        kwargs = {}

        if attype == 'numeric':
            if charlen:
                prec, scale = charlen.split(',')
                args = (int(prec), int(scale))
            else:
                args = ()
        elif attype == 'double precision':
            args = (53, )
        elif attype == 'integer':
            args = ()
        elif attype in ('timestamp with time zone',
                        'time with time zone'):
            kwargs['timezone'] = True
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif attype in ('timestamp without time zone',
                        'time without time zone', 'time'):
            kwargs['timezone'] = False
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif attype == 'bit varying':
            kwargs['varying'] = True
            if charlen:
                args = (int(charlen),)
            else:
                args = ()
        elif attype in ('interval', 'interval year to month',
                        'interval day to second'):
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif charlen:
            args = (int(charlen),)

        coltype = ischema_names.get(attype, None)
        if coltype:
            coltype = coltype(*args, **kwargs)
            if is_array:
                coltype = ARRAY(coltype)
        else:
            coltype = sqltypes.NULLTYPE
        return coltype
