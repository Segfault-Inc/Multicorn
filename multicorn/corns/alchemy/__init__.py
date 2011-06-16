from __future__ import print_function
from ..abstract import AbstractCorn
from ...requests.types import Type

from . import dialects

try:
    import sqlalchemy
except ImportError:
    import sys
    print("WARNING: The SQLAlchemy AP is not available.", file=sys.stderr)
else:
    from sqlalchemy import create_engine, Table, Column, MetaData


class Alchemy(AbstractCorn):

    def __init__(self, name, identity_properties=None, url="sqlite:///",
            tablename=None, schema=None, engine_opts=None, create_table=True):
        super(Alchemy, self).__init__(name, identity_properties)
        self.__table = None
        self.url = url
        self.tablename = tablename or name
        self.schema = schema
        self.engine_opts = engine_opts or {}
        self.create_table = True

    def bind(self, multicorn):
        super(Alchemy, self).bind(multicorn)
        if not hasattr(self.multicorn, '_alchemy_metadatas'):
            self.multicorn._alchemy_metadatas = {}
        metadata = self.multicorn._alchemy_metadatas.get(self.url, None)
        if metadata is None:
            engine = create_engine(self.url, **self.engine_opts)
            metadata = MetaData()
            metadata.bind = engine
            self.multicorn._alchemy_metadatas[self.url] = metadata
        self.metadata = metadata
        # TODO: manage dialect creation here
        self.dialect = dialects.get_dialect(engine)

    def register(self, name, type):
        self.properties[name] = Type(corn=self, name=name, type=type)

    @property
    def table(self):
        if self.__table is not None:
            return self.__table
        columns = []
        for prop in self.properties.values():
            kwargs = {}
            if prop.name in self.identity_properties:
                kwargs["primary_key"] = True
            type = self.dialect.alchemy_type(prop)
            column = Column(prop.name, type, **kwargs)
            columns.append(column)
        kwargs = dict(useexisting=True)
        if self.schema:
            kwargs['schema'] = self.schema
        table = Table(self.tablename, self.metadata, *columns,
                **kwargs)
        if self.create_table == True:
            table.create()
        self.__table = table
        return table

    def _all(self):
        return self.table.select().execute()
