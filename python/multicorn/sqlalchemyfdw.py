"""A SQLAlchemy foreign data wrapper"""

from . import ForeignDataWrapper, TableDefinition, ColumnDefinition
from .utils import log_to_postgres, ERROR, WARNING, DEBUG
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.sql import select, operators as sqlops, and_
# Handle the sqlalchemy 0.8 / 0.9 changes
try:
    from sqlalchemy.sql import sqltypes
except ImportError:
    from sqlalchemy import types as sqltypes

from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.dialects.oracle import base as oracle_dialect
from sqlalchemy.dialects.postgresql.base import (
    ARRAY, ischema_names, PGDialect, NUMERIC)
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


def _parse_url_from_options(fdw_options):
    if fdw_options.get('db_url'):
        url = make_url(fdw_options.get('db_url'))
    else:
        if 'drivername' not in fdw_options:
            log_to_postgres('Either a db_url, or drivername and other '
                            'connection infos are needed', ERROR)
        url = URL(fdw_options['drivername'])
    for param in ('username', 'password', 'host',
                    'database', 'port'):
        if param in fdw_options:
            setattr(url, param, fdw_options[param])
    return url



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

CONVERSION_MAP = {
    oracle_dialect.NUMBER: NUMERIC
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
        if 'tablename' not in fdw_options:
            log_to_postgres('The tablename parameter is required', ERROR)
        self.metadata = MetaData()
        url = _parse_url_from_options(fdw_options)
        self.engine = create_engine(url)
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
            columns = self.table.c
        statement = statement.with_only_columns(columns)
        log_to_postgres(str(statement), DEBUG)
        rs = (self.connection
              .execution_options(stream_results=True)
              .execute(statement))
        for item in rs:
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
        # strip (*) from character varying(5), timestamp(5)
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

    @classmethod
    def import_schema(self, schema, srv_options, options,
                      restriction_type, restricts):

        metadata = MetaData()
        url = _parse_url_from_options(srv_options)
        engine = create_engine(url)
        dialect = PGDialect()
        if restriction_type == 'limit':
            only = restricts
        elif restriction_type == 'except':
            only = lambda t, _: t not in restricts
        else:
            only = None
        metadata.reflect(bind=engine,
                         schema=schema,
                         only=only)
        to_import = []
        for _, table in sorted(metadata.tables.items()):
            ftable = TableDefinition(table.name)
            ftable.options['schema'] = schema
            ftable.options['tablename'] = table.name
            for c in table.c:
                # Force collation to None to prevent imcompatibilities
                setattr(c.type, "collation", None)
                # If the type is specialized, call the generic
                # superclass method
                if type(c.type) in CONVERSION_MAP:
                    class_name = CONVERSION_MAP[type(c.type)]
                    old_args = c.type.__dict__
                    c.type = class_name()
                    c.type.__dict__.update(old_args)
                if c.primary_key:
                    ftable.options['primary_key'] = c.name
                ftable.columns.append(ColumnDefinition(
                    c.name,
                    type_name=c.type.compile(dialect)))
            to_import.append(ftable)
        return to_import
