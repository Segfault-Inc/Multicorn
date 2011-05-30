# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Alchemy
=======

Access point storing items in a RDBMS.

"""

from __future__ import print_function
from datetime import datetime, date
from decimal import Decimal
from functools import reduce

from .. import AccessPoint
from ...request import Condition, And, Or, Not, RequestProperty, make_request_property
from ...property import Property
from ... import query as kquery
from ...value import to_unicode


try:
    import sqlalchemy
except ImportError:
    import sys
    print("WARNING: The SQLAlchemy AP is not available.", file=sys.stderr)
else:
    from sqlalchemy import create_engine, Table, Column, MetaData, ForeignKey, \
        Integer, Date, Numeric, DateTime, Boolean, Unicode
    from sqlalchemy.sql import expression, and_, or_, not_
    import sqlalchemy.exc

    from . import querypatch

    from . import dialect

    SQLALCHEMYTYPES = {
        unicode: Unicode,
        bytes: Unicode,
        int: Integer,
        datetime: DateTime,
        date: Date,
        bool: Boolean,
        Decimal: Numeric}

    And.alchemy_function = lambda self, conditions: and_(*conditions)
    Or.alchemy_function = lambda self, conditions: or_(*conditions)
    Not.alchemy_function = lambda self, conditions: not_(*conditions)

DEFAULT_VALUE = object()


class AlchemyProperty(Property):
    """Property for an Alchemy access point."""


    DB_GEN = (DEFAULT_VALUE,)

    def __init__(self, property_type, column_name=None, **kwargs):
        super(AlchemyProperty, self).__init__(property_type, **kwargs)
        self.column_name = column_name
        self.column = None


class QueryNode(object):

    def __init__(self, property=None, parent=None):
        self.property = property
        self.parent = parent
        self.children = {}
        self.name = None
        self.selects = []
        self.table = self.property.remote_ap._table.alias()\
                if self.property else None


    def add_child(self, property):
        return self.children.setdefault(property.name,
                QueryNode(property, self))

    def find_table(self, property):
        if property.child_property:
           return self.children[property.name].find_table(property.child_property)
        matching = list(filter(lambda x: x.name == property.name, self.selects))
        if matching:
            return matching[0]._element
        if self.property == property:
            return self.table.c[property.name]
        # Bruteforce on aliases
        for child in self.children.values():
            col = child.find_table(property)
            if col is not None:
                return col
        return self.table.c.get(property.name, None)




class Alchemy(AccessPoint):
    """Access point used to store data in a RDBMS."""
    __metadatas = {}

    def __init__(self, url, tablename, properties, identity_properties,
                 createtable=False, engine_opts = None, schema=None):
        super(Alchemy, self).__init__(properties, identity_properties)
        self.__table = None
        self.url = url
        self.tablename = tablename
        self.createtable = createtable
        self.remote_alchemy_props = []
        self.metadata = None
        self.schema = schema
        self.engine_opts = engine_opts or {}
        for name, prop in self.properties.items():
            if prop.column_name is None:
                prop.column_name = name
            if prop.relation is None:
                self._column_from_prop(prop)

    def _column_from_prop(self, prop):
        """For a given property, return the alchemy Column instance.

        If the instance has not been created yet, create it.

        """
        if prop.column is not None:
            return prop.column
        alchemy_type = SQLALCHEMYTYPES.get(prop.type, None)
        kwargs = {"key": prop.name}
        if prop.name in (idprop.name for idprop in self.identity_properties):
            kwargs["primary_key"] = True
        if prop.relation == "many-to-one":
            foreign_ap = prop.remote_ap
            # Transpose the kalamar relation in alchemy if possible
            if isinstance(foreign_ap, Alchemy):
                foreign_table = foreign_ap.tablename
                if foreign_ap is not self:
                    foreign_ap._table
                if foreign_ap.schema:
                    foreign_table = "%s.%s" % (foreign_ap.schema, foreign_table)
                # TODO: Fix this for circular dependencies
                foreign_column = self._get_column(
                    "%s.%s" % (prop.name, prop.remote_property.name))
                foreign_name = "%s.%s" % (foreign_table, foreign_column.key)
                foreign_key = ForeignKey(
                    foreign_name, use_alter=True,
                    name="%s_%s_fkey" % (self.tablename, prop.name))
                self.remote_alchemy_props.append(prop.name)
                alchemy_type = foreign_column.type
                column = Column(
                    prop.column_name, alchemy_type, foreign_key, **kwargs)
            else:
                if len(foreign_ap.identity_properties) == 1:
                    foreign_prop = foreign_ap.identity_properties[0]
                    alchemy_type = alchemy_type or \
                        SQLALCHEMYTYPES.get(foreign_prop.type, None)
                else:
                    alchemy_type = SQLALCHEMYTYPES.get(unicode)
                column = Column(prop.column_name, alchemy_type, **kwargs)
        elif prop.relation == "one-to-many":
            # TODO: Manage multiple foreign-key
            column = self.identity_properties[0].column
        else:
            column = Column(prop.column_name, alchemy_type, **kwargs)
        prop.column = column
        return column

    @property
    def _table(self):
        """Initialize the Alchemy engine on first access."""
        if self.__table is not None:
            return self.__table

        metadata = Alchemy.__metadatas.get(self.url, None)
        if not metadata:
            engine = create_engine(self.url, **self.engine_opts)
            metadata = MetaData()
            metadata.bind = engine
            Alchemy.__metadatas[self.url] = metadata
        self.metadata = metadata
        self.dialect = dialect.get_dialect(metadata.bind.dialect)


        # We must do 3 things here:
        # - Call _column_from_prop on all props to ensure linkage with remote
        #   columns;
        # - Add all real columns (all columns but one-to-many ones) to Table;
        # - Keep the order of self.identity_properties in Table.
        real_non_identity_columns = []
        for prop in self.properties.values():
            if prop.name not in (idprop.name
                    for idprop in self.identity_properties):
                column = self._column_from_prop(prop)
                if prop.relation != "one-to-many":
                    real_non_identity_columns.append(column)
        identity_columns = [
            self._column_from_prop(prop) for prop in self.identity_properties]
        kwargs = dict(useexisting=True)
        if self.schema:
            kwargs['schema'] = self.schema
        table = Table(
            self.tablename, metadata,
            *(identity_columns + real_non_identity_columns),
            **kwargs)
        if self.createtable:
            table.create(checkfirst=True)
        self.__table = table
        return table

    def _get_column(self, property_name):
        """For a given ``property``, return the (possibly foreign) column."""
        splitted = property_name.split(".")
        prop = self.properties[splitted[0]]
        if len(splitted) > 1:
            # _get_column isn't really protected but shared between Alchemy APs
            # pylint: disable=W0212
            return prop.remote_ap._get_column(".".join(splitted[1:]))
            # pylint: enable=W0212
        else:
            return prop.column

    def __item_from_result(self, result):
        """Create an item from a result line."""
        lazy_props = {}
        props = {}
        lazy_to_build = []
        for name, prop in self.properties.items():
            if prop.relation == "one-to-many":
                lazy_to_build.append(prop)
            elif prop.relation == "many-to-one" and result[name] is not None:
                lazy_props[name] = self._many_to_one_lazy_loader(
                    prop, result[name])
            else:
                props[name] = result[name]
        for prop in lazy_to_build:
            lazy_props[prop.name] = self._default_loader(props, prop)
        item = self.create(props, lazy_props)
        item.saved = True
        return item

    def _many_to_one_lazy_loader(self, prop, value):
        """Create a lazy loader for many_to_ones properties.

        For one to many properties, the loader is built by the base access
        point directly.

        """
        return prop.remote_ap.loader_from_reference_repr(to_unicode(value))

    def search(self, request):
        return (self.__item_from_result(line)
                for line in self.view(kquery.QueryFilter(request)))

    def __to_pk_where_clause(self, item):
        """Build an alchemy condition matching this item on its pks."""
        tree = QueryNode()
        tree.table = self._table
        return querypatch._build_where(self, And(*[
                    Condition(prop.name, "=", item[prop.name])
                    for prop in self.identity_properties]), tree)

    def __transform_to_table(self, item):
        """Transform an item to a dict so that it can be saved."""
        item_dict = {}
        for name, prop in self.properties.items():
            if prop.auto == AlchemyProperty.DB_GEN\
                    and item[name] == DEFAULT_VALUE:
                continue
            if prop.relation == "many-to-one":
                if item[name] is None:
                    item_dict[name] = None
                else:
                    item_dict[name] = item[name].reference_repr()
            elif prop.relation != "one-to-many":
                item_dict[name] = item[name]
        return item_dict

    def save(self, item):
        connection = self._table.bind.connect()
        transaction = connection.begin()
        value = self.__transform_to_table(item)
        try:
            # Try to insert
            statement = self._table.insert().values(value).execute()
            for (gen_id, id_prop) in zip(
                statement.inserted_primary_key, self.identity_properties):
                item[id_prop.name] = gen_id
            transaction.commit()
        except sqlalchemy.exc.IntegrityError:
            # A line already exists with these primary keys, try to update
            try:
                whereclause = self.__to_pk_where_clause(item)
                update = self._table.update()
                rows = update.where(whereclause).values(value).execute()
                if rows.rowcount == 0: # pragma: no cover
                    # Insert failed but no line to update
                    raise
                transaction.commit()
            except: # pragma: no cover
                # No way found to insert nor update
                transaction.rollback()
                raise RuntimeError(
                    "An error occured while saving an item in the database")
        finally:
            connection.close()
        item.saved = True

    def delete(self, item):
        whereclause = self.__to_pk_where_clause(item)
        self._table.delete().where(whereclause).execute()

    def extract_properties_select(self, query, properties, tree):
        def append_select(tree, property, name):
            selectable = self.dialect.get_selectable(property, tree)
            tree.selects.append(selectable.label(name))

        for name, value in query.mapping.items():
            if value.name == '*':
                for pname, prop in properties.items():
                    if prop.relation != 'one-to-many':
                        prop = make_request_property(pname)
                        append_select(tree, prop, ''.join((name, pname)))
            else:
                append_select(tree, value, name)
        for prop, sub_select in query.sub_selects.items():
            return_property = prop.return_property(properties)
            # If we are just fetching the identity properties, we do not
            # need a join
            remote_id_props_names = [fp.name
                    for fp in return_property.remote_ap.identity_properties]
            if all((submapping.name in remote_id_props_names
                for submapping in sub_select.mapping.values()))\
                    and not sub_select.sub_selects\
                    and return_property.relation != 'one-to-many':
                for alias, subprop in sub_select.mapping.items():
                    append_select(tree, prop, alias)
            else:
                # If we are taking this branch with an instance
                assert isinstance(return_property.remote_ap, Alchemy),\
                "Not an Alchemy access point. Something went wrong during the validation"
                node = tree.add_child(return_property)
                self.extract_properties_select(sub_select, return_property.remote_ap.properties, node)

    def extract_properties_aggregate(self, query, properties, tree):
        for grouper in query.groupers:
            selectable = self.dialect.get_selectable(grouper, tree)
            tree.selects.append(selectable.label(grouper.name))

    def extract_properties_filter(self, query, properties, tree):
        def extract_properties_from_tree(properties_tree, properties, tree):
            for name, value in properties_tree.items():
                # Unalias the property
                prop = properties[name]
                if prop.remote_ap:
                    remote_ids = prop.remote_ap.identity_properties
                    if hasattr(value, "child_property"):
                        # If there is no descendent, or descendent is an
                        # identity property, don't do a join
                        if value.child_property is None:
                            continue
                        if value in [RequestProperty(id_prop.name)
                                for id_prop in remote_ids]:
                            continue
                    node = tree.add_child(prop)
                    if hasattr(value, "items"):
                        extract_properties_from_tree(value, prop.remote_ap.properties, node)
        extract_properties_from_tree(query.condition.properties_tree, properties,
                tree)

    def extract_properties(self, query, properties, tree):
        if isinstance(query, kquery.QuerySelect):
            self.extract_properties_select(query, properties, tree)
        elif query.__class__ ==  kquery.QueryFilter:
            self.extract_properties_filter(query, properties, tree)
        elif isinstance(query, kquery.QueryAggregate):
            self.extract_properties_aggregate(query, properties, tree)
        elif isinstance(query, kquery.QueryChain):
            for sub in query.queries:
                self.extract_properties(sub, properties, tree)
                properties = sub.validate(properties)


    def _build_join(self, query, properties):
        tree = QueryNode()
        tree.table = self._table
        # Build the tree
        self.extract_properties(query, properties, tree)
        def inner_build_join(join, tree):
            for name, node in tree.children.items():
                if node.property:
                    if node.table.element != join:
                        col1 = node.parent.table.c[node.property.column.key]
                        col2 = node.table.c[node.property.remote_property.column.key]
                        join = join.outerjoin(node.table, onclause = col1 == col2)
                join = inner_build_join(join, node)
            return join
        query = inner_build_join(self._table, tree)
        return query, tree

    def _build_select(self, tree):
        return reduce(list.__add__, (self._build_select(child)
            for child in tree.children.values()), tree.selects)

    def view(self, kalamar_query):
        """ Only kalamar_queries of the form :
            SELECT --> FILTER --> ORDER BY --> RANGE (everything optional) are
            managed by this access_point.
            Else, we fall back on "software" joins.
        """
        # Initialize the table before anything
        self._table
        properties = kalamar_query.validate(self.properties)
        can, cants = kalamar_query.alchemy_validate(self, self.properties)
        if can:
            join, tree = self._build_join(can, self.properties)
            alchemy_query = expression.select(columns=self._build_select(tree), from_obj=join)
            for select in self._build_select(tree):
                alchemy_query.append_column(select)
            alchemy_query = can.to_alchemy(self, tree, alchemy_query)
            # If no column were added to the select clause, select
            # everything
            if not alchemy_query.c:
                for name, prop in self.properties.items():
                    if prop.relation != 'one-to-many':
                        alchemy_query.append_column(prop.column.label(name))
            self.site.logger.debug(alchemy_query)
            result = alchemy_query.execute()
        else:
            query = expression.Select(
                None, None, from_obj=self._table)
            for name, prop in self.properties.items():
                if prop.relation != "one-to-many":
                    query.append_column(prop.column.label(name))
            result = (self.__item_from_result(line) for line in query.execute())
        # In the generic case, reduce the conversational overhead.
        # If someone uses only a subset of the result, then build the query
        # accordingly!
        cants = cants or kquery.QueryChain([])
        result = list(result)
        for line in cants(result):
            new_line = {}
            for key, value in line.items():
                prop = properties[key]
                new_line[key] = prop.cast((value,))[0]
            yield new_line
