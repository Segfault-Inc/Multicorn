from __future__ import print_function
from ..abstract import AbstractCorn
from ...requests.types import Type, List, Dict
from ... import python_executor


try:
    import sqlalchemy
except ImportError:
    import sys
    print("WARNING: The SQLAlchemy AP is not available.", file=sys.stderr)
else:
    from sqlalchemy import create_engine, Table, Column, MetaData
    from sqlalchemy import sql as sqlexpr


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
        self.dialect = get_dialect(engine)

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

    def _to_pk_where_clause(self, item):
        conditions = []
        for key in self.identity_properties:
             conditions.append(self.table.columns[key] == item[key])
        return sqlexpr.and_(*conditions)

    def save(self, item):
        connection = self.table.bind.connect()
        transaction = connection.begin()
        # Try to open the item
        where_clause = self._to_pk_where_clause(item)
        statement = self.table.select().where(where_clause)
        results = statement.execute()
        try:
            iter(results).next()
            # The item exists, it's an UPDATTE
            rows = self.table.update().where(where_clause).values(dict(item)).execute()
            if rows.rowcount > 1:
                transaction.rollback()
                raise ValueError("There is more than one item to update!")
            transaction.commit()
        except StopIteration:
            # The item does not exist, it's an INSERT
            statement = self.table.insert().values(dict(item)).execute()
            transaction.commit()

    def delete(self, item):
        connection = self.table.bind.connect()
        transaction = connection.begin()
        result = self._table.delete().where(
                self._to_pk_where_clause(item)).execute()
        if result.rowcount > 1:
            transaction.rollback()
            raise ValueError("There is more than one item to delete!")

    def _is_same_db(self, other_type):
        return other_type.corn is None or (isinstance(other_type.corn, Alchemy)\
                and other_type.corn.url == self.url)

    def is_all_alchemy(self, request, contexts=()):
        used_types = request.used_types()
        all_requests = reduce(lambda x, y: list(x) + list(y), used_types.values(), set())
        return all(isinstance(x, self.dialect.RequestWrapper) for x in all_requests) and\
                all(self._is_same_db(x) for x in used_types.keys())

    def _transform_result(self, result, return_type):
        def process_list(result):
            for item in result:
                yield self._transform_result(item, return_type.inner_type)
        if isinstance(return_type, List):
            return process_list(result)
        elif isinstance(return_type, Dict):
            if return_type.type == self.type:
                return self.create(result)
            else:
                return result
        elif isinstance(return_type, Dict):
            return result


    def execute(self, request, contexts=()):
        wrapped_request = self.dialect.wrap_request(request)
        if self.is_all_alchemy(wrapped_request, contexts):
            sql_query = wrapped_request.to_alchemy(None, contexts)
            return_type = wrapped_request.return_type()
            return self._transform_result(sql_query.execute(), return_type)
        else:
            return python_executor.execute(request)


from .dialects import get_dialect


