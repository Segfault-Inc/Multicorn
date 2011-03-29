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

from .. import AccessPoint
from ...item import AbstractItem, Item
from ...request import Condition, And, Or, Not
from ...property import Property
from ...query import QueryChain
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
                if foreign_ap.schema:
                    foreign_table = "%s.%s" % (foreign_ap.schema, foreign_table)
                # TODO: Fix this for circular dependencies
                foreign_column = self._get_column(
                    "%s.%s" % (prop.name, prop.remote_property.name))
                foreign_name = "%s.%s" % (foreign_table, foreign_column.name)
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

    def to_alchemy_condition(self, condition):
        """Convert a kalamar condition to an sqlalchemy condition."""
        if isinstance(condition, (And, Or, Not)):
            alchemy_conditions = tuple(
                self.to_alchemy_condition(sub_condition)
                for sub_condition in condition.sub_requests)
            return condition.alchemy_function(alchemy_conditions)
        else:
            column = self._get_column(condition.property.name)
            value = condition.value
            if isinstance(value, AbstractItem):
                value = value.reference_repr()
            if condition.operator == "=":
                return column == value
            elif condition.operator == "!=":
                return column != value
            # TODO: Enhance the condition handling to manage '~='
            # on other systems
            elif condition.operator == "like":
                return column.like(value)
            else:
                return column.op(condition.operator)(value)

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
        query = expression.Select(
            None, None, from_obj=self._table, use_labels=True)
        query.append_whereclause(self.to_alchemy_condition(request))
        for name, prop in self.properties.items():
            if prop.relation != "one-to-many":
                query.append_column(prop.column.label(name))
        execution = query.execute()
        return (self.__item_from_result(line) for line in execution)

    def __to_pk_where_clause(self, item):
        """Build an alchemy condition matching this item on its pks."""
        return self.to_alchemy_condition(And(*[
                    Condition(prop.name, "=", item[prop.name])
                    for prop in self.identity_properties]))

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

    def view(self, kalamar_query):
        alchemy_query = expression.select(from_obj=self._table)
        can, cants = kalamar_query.alchemy_validate(self, self.properties)
        if can:
            alchemy_query = can.to_alchemy(alchemy_query, self, self.properties)
            # If no column were added to the select clause, select
            # everything
            if not alchemy_query.c:
                for name, prop in self.properties.items():
                    if prop.relation != 'one-to-many':
                        alchemy_query.append_column(prop.column.label(name))
            result = alchemy_query.execute()
        else:
            result = self.search(And())
        # In the generic case, reduce the conversational overhead.
        # If someone uses only a subset of the result, then build the query
        # accordingly!
        properties = kalamar_query.validate(self.site, self.properties)
        cants = cants or QueryChain([])
        for line in cants(result):
            new_line = {}
            for key, value in line.items():
                prop = properties[key]
                new_line[key] = prop.cast((value,))[0]
            yield new_line
