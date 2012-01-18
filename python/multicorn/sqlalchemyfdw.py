"""An SQLite 3 foreign data wrapper"""

from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR, WARNING
from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.dialects.postgresql.base import ischema_names


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
        for item in self.engine.execute(statement):
            yield item
