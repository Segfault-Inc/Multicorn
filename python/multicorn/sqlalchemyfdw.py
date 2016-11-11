"""
Purpose
-------

This fdw can be used to access data stored in a remote RDBMS.
Through the use of sqlalchemy, many different rdbms engines are supported.

.. api_compat::
    :read:
    :write:
    :transaction:
    :import_schema:


Dependencies
------------

You will need the `sqlalchemy`_ library, as well as a suitable dbapi driver for
the remote database.

You can find a list of supported RDBMs, and their associated dbapi drivers and
connection strings in the `sqlalchemy dialects documentation`_.

.. _sqlalchemy dialects documentation: http://docs.sqlalchemy.org/en/latest/dialects/

.. _sqlalchemy: http://www.sqlalchemy.org/

Connection options
~~~~~~~~~~~~~~~~~~

Connection options can be passed either with a db-url, or with a combination of
individual connection parameters.
If both a ``db_url`` and individual parameters are used, the parameters will override
the value found in the ``db_url``.
In both cases, at least the ``drivername`` should be passed, either as the url scheme in
the ``db_url`` or using the ``drivername`` parameter.

``db_url``
  An sqlalchemy connection string.
  Examples:

    - mysql: `mysql://<user>:<password>@<host>/<dbname>`
    - mssql: `mssql://<user>:<password>@<dsname>`

  See the `sqlalchemy dialects documentation`_. for documentation.

``username``
  The remote username.

``password``
  The remote password

``host``
  The remote host

``database``
  The remote database

``port``
  The remote port


Other options
---------------

``tablename`` (required)
  The table name in the remote RDBMS.

``primary_key``
  Identifies a column which is a primary key in the remote RDBMS.
  This options is required for INSERT, UPDATE and DELETE operations

``schema``
  The schema in which this table resides on the remote side

When defining the table, the local column names will be used to retrieve the
remote column data.
Moreover, the local column types will be used to interpret the results in the
remote table. Sqlalchemy being aware of the differences between database
implementations, it will convert each value from the remote database to python
using the converter inferred from the column type, and convert it back to a
postgresql suitable form.

What does it do to reduce the amount of fetched data ?
------------------------------------------------------

- `quals` are pushed to the remote database whenever possible. This include
  simple operators :

    - equality, inequality (=, <>, >, <, <=, >=)
    - like, ilike and their negations
    - IN clauses with scalars, = ANY (array)
    - NOT IN clauses, != ALL (array)
- the set of needed columns is pushed to the remote_side, and only those columns
  will be fetched.

Sort push-down support
----------------------

Since the rules about NULL ordering are different for every database vendor, and
many of them don't support the NULLS FIRST, NULLS LAST clause, this FDW tries
to not generate any NULLS FIRST / LAST clause if the requested order matches
what the remote system would do by default.

Additionnaly, if it is found that a query can't be executed while keeping the
same NULL ordering (because the remote system doesn't support the NULL ordering
clause), the sort will not be pushed down.

To check the SQL query that will be sent to the remote system, use EXPLAIN:

.. code-block:: sql

    postgres=# explain select * from testalchemy  order by id DESC NULLS FIRST;
                                                        QUERY PLAN
    ------------------------------------------------------------------------------------------------------------------
    Foreign Scan on testalchemy  (cost=20.00..50000000000.00 rows=100000000 width=500)
    Multicorn: SELECT basetable.atimestamp, basetable.anumeric, basetable.adate, basetable.avarchar, basetable.id
    FROM basetable ORDER BY basetable.id DESC
    (3 lignes)

    Temps : 167,856 ms
    postgres=# explain select * from testalchemy  order by id DESC NULLS LAST;
                                                        QUERY PLAN
    ------------------------------------------------------------------------------------------------------------------
    Foreign Scan on testalchemy  (cost=20.00..50000000000.00 rows=100000000 width=500)
    Multicorn: SELECT basetable.atimestamp, basetable.anumeric, basetable.adate, basetable.avarchar, basetable.id
    FROM basetable ORDER BY basetable.id DESC NULLS LAST
    (3 lignes)


Usage example
-------------

For a connection to a remote mysql database (you'll need a mysql dbapi driver,
such as pymysql):

.. code-block:: sql

  CREATE SERVER alchemy_srv foreign data wrapper multicorn options (
      wrapper 'multicorn.sqlalchemyfdw.SqlAlchemyFdw'
  );

  create foreign table mysql_table (
    column1 integer,
    column2 varchar
  ) server alchemy_srv options (
    tablename 'table',
    db_url 'mysql://myuser:mypassword@myhost/mydb'
  );

"""

from . import ForeignDataWrapper, TableDefinition, ColumnDefinition
from .utils import log_to_postgres, ERROR, WARNING, DEBUG
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.sql import select, operators as sqlops, and_
from sqlalchemy.sql.expression import nullsfirst, nullslast

# Handle the sqlalchemy 0.8 / 0.9 changes
try:
    from sqlalchemy.sql import sqltypes
except ImportError:
    from sqlalchemy import types as sqltypes

from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.dialects.oracle import base as oracle_dialect
from sqlalchemy.dialects.postgresql.base import ( ischema_names, PGDialect, NUMERIC)
from sqlalchemy.dialects.postgresql.array import ARRAY
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

SORT_SUPPORT = {
    'mssql': {'default': 'lower', 'support': False},
    'postgresql': {'default': 'higher', 'support': True},
    'mysql': {'default': 'lower', 'support': False},
    'oracle': {'default': 'higher', 'support': True},
    'sqlite': {'default': 'lower', 'support': False}
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



    def _need_explicit_null_ordering(self, key):
        support = SORT_SUPPORT[self.engine.dialect.name]
        default = support['default']
        no = None
        if key.is_reversed:
            no = nullsfirst if default == 'higher' else nullslast
        else:
            no = nullslast if default == 'higher' else nullsfirst
        if key.nulls_first:
            if no != nullsfirst:
                return nullsfirst
            return None
        else:
            if no != nullslast:
                return nullslast
            return None

    def can_sort(self, sortkeys):
        if SORT_SUPPORT.get(self.engine.dialect.name) is None:
            # We have no idea about defaults
            return []
        can_order_null = SORT_SUPPORT[self.engine.dialect.name]['support']
        if (any((self._need_explicit_null_ordering(x) is not None
                 for x in sortkeys)) and not can_order_null):
            return []
        return sortkeys

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        sortkeys = sortkeys or []
        statement = self._build_statement(quals, columns, sortkeys)
        return [str(statement)]

    def _build_statement(self, quals, columns, sortkeys):
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
        orders = []
        for sortkey in sortkeys:
            column = self.table.c[sortkey.attname]
            if sortkey.is_reversed:
                column = column.desc()
            if sortkey.collate:
                column = column.collate('"%s"' % sortkey.collate)
            null_ordering = self._need_explicit_null_ordering(sortkey)
            if null_ordering:
                column = null_ordering(column)
            statement = statement.order_by(column)
        return statement


    def execute(self, quals, columns, sortkeys=None):
        """
        The quals are turned into an and'ed where clause.
        """
        sortkeys = sortkeys or []
        statement = self._build_statement(quals, columns, sortkeys)
        log_to_postgres(str(statement), DEBUG)
        rs = (self.connection
              .execution_options(stream_results=True)
              .execute(statement))
        # Workaround pymssql "trash old results on new query"
        # behaviour (See issue #100)
        if self.engine.driver == 'pymssql' and self.transaction is not None:
            rs = list(rs)

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
        """
        Reflects the remote schema.
        """
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
