from __future__ import print_function
from ..abstract import AbstractCorn
from . import dialects

try:
    import sqlalchemy
except ImportError:
    import sys
    print("WARNING: The SQLAlchemy AP is not available.", file=sys.stderr)
else:
    from sqlalchemy import create_engine, Table, Column, MetaData, ForeignKey, \
        Integer, Date, Numeric, DateTime, Boolean, Unicode


class AlchemyCorn(AbstractCorn):
    """
    An access point for managing relational database through sqlalchemy
    """

    __metadatas = {}


    def __init__(self, properties=None, identity_properties=None, url=None, tablename=None,
            create_table=False,
            engine_opts=None, schema=None):
        self.__table == None
        self.url = url
        self.tablename = tablename
        self.create_table = create_table
        self.metadata = None
        self.schema = schema
        self.engine_opts = engine_opts
        if properties:
            pass
        else:
            self.properties = {}


    @property
    def table(self):
        """Initialize the Alchemy engine on first access."""
        if self.__table is not None:
            return self.__table
        metadata = AlchemyCorn.__metadatas.get(self.url, None)
        if not metadata:
            engine = create_engine(self.url, **self.engine_opts)
            metadata = MetaData()
            metadata.bind = engine
            AlchemyCorn.__metadatas[self.url] = metadata
        self.metadata = metadata
        self.dialect = dialect.get_dialect(metadata.engine)
        # TODO: create the table





