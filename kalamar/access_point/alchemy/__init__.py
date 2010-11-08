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

from werkzeug.utils import cached_property
from sqlalchemy import create_engine, Table, Column, MetaData, ForeignKey, \
    Integer, Date, Numeric, DateTime, Boolean, Unicode
from sqlalchemy.sql import expression, and_, or_, not_
from datetime import datetime, date
from decimal import Decimal
import sqlalchemy.sql.expression 

from . import querypatch
from .. import AccessPoint
from ...item import Item
from ...request import Condition, And, Or, Not
from ...query import QueryChain
from ...property import Property


SQLALCHEMYTYPES = {
    unicode: Unicode,
    int: Integer,
    datetime: DateTime,
    date: Date,
    bool: Boolean,
    Decimal: Numeric}

And.alchemy_function = lambda self, conditions: and_(*conditions)
Or.alchemy_function = lambda self, conditions: or_(*conditions)
Not.alchemy_function = lambda self, conditions: not_(*conditions)


class AlchemyProperty(Property):
    """Property for an Alchemy access point."""
    def __init__(self, property_type, column_name, **kwargs):
        super(AlchemyProperty, self).__init__(property_type, **kwargs)
        self.column_name = column_name
        self.column = None


class Alchemy(AccessPoint):
    """Access point used to store data in a RDBMS."""
    __metadatas = {}

    def __init__(self, url, tablename, properties, identity_properties, 
                 createtable=False):
        super(Alchemy, self).__init__(properties, identity_properties)
        self.url = url
        self.tablename = tablename
        self.createtable = createtable
        self.remote_alchemy_props = []
        self.metadata = None
        for name, prop in self.properties.items():
            prop.name = name 
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
        if prop in self.identity_properties:
            kwargs["primary_key"] = True
        if prop.relation == "many-to-one":
            foreign_ap = prop.remote_ap
            #Transpose the kalamar relation in alchemy if possible
            if isinstance(foreign_ap, Alchemy):
                foreign_table = foreign_ap.tablename
                #TODO: Fix this for circular dependencies
                foreign_column = self.__get_column("%s.%s" % (prop.name,
                    prop.remote_property.name))
                foreign_name = "%s.%s" % (foreign_table, foreign_column)
                foreign_key = ForeignKey(foreign_name, use_alter = True, 
                        name = "%s_%s_fkey" % (self.tablename, prop.name ))
                self.remote_alchemy_props.append(prop.name)
                alchemy_type = foreign_column.type
                column = Column(
                    prop.column_name, alchemy_type, foreign_key, **kwargs)
            else:
                #TODO: manage multiple foreign_keys
                foreign_prop = foreign_ap.identity_properties[0]
                alchemy_type = alchemy_type or \
                    SQLALCHEMYTYPES.get(foreign_prop.type, None)
                column = Column(prop.column_name, alchemy_type, **kwargs)
        elif prop.relation == "one-to-many":
            #TODO manage multiple foreign-key
            column = self.identity_properties[0].column
        else:
            column = Column(prop.column_name, alchemy_type, **kwargs)
        prop.column = column
        return column

    # TODO: remove the werkzeug depedency
    @cached_property
    def _table(self):
        """Initialize the Alchemy engine on first access."""
        metadata = Alchemy.__metadatas.get(self.url, None)
        if not metadata:
            engine = create_engine(self.url, echo=True)
            metadata = MetaData()
            metadata.bind = engine
            Alchemy.__metadatas[self.url] = metadata
        self.metadata = metadata
        columns = set([self._column_from_prop(prop) for prop in
                   self.properties.values()])
        table = Table(self.tablename, metadata, *columns, useexisting=True)
        if self.createtable:
            table.create()
        return table

    def __get_column(self, property_name):
        """For a given ``property``, return the (possibly foreign) column."""
        splitted = property_name.split(".")
        prop = self.properties[splitted[0]]
        if len(splitted) > 1:
            # __get_column isn't really protected but shared between Alchemy APs
            # pylint: disable=W0212
            return prop.remote_ap.__get_column(".".join(splitted[1:]))
            # pylint: enable=W0212
        else:
            return prop.column

    def to_alchemy_condition(self, condition):
        """Converts a kalamar condition to an sqlalchemy condition."""
        if isinstance(condition, (And, Or, Not)):
            alchemy_conditions = tuple(
                self.to_alchemy_condition(sub_condition)
                for sub_condition in condition.sub_requests)
            return condition.alchemy_function(alchemy_conditions)
        else:
            column = self.__get_column(condition.property.name)
            value = condition.value
            if value.__class__ == Item:
                #TODO: manage multiple foreign key
                value = value.identity.condition.value
            if condition.operator == "=":
                return column == value
            else:
                return column.op(condition.operator)(value)
        
    def __item_from_result(self, result):
        """Creates an item from a result line."""
        lazy_props = {}
        props = {}
        for name, prop in self.properties.items():
            if prop.relation == "one-to-many":
                lazy_props[name] = None
            elif prop.relation == "many-to-one" and result[name] is not None:
                lazy_props[name] = self._many_to_one_lazy_loader(prop, 
                        result[name])
            else: 
                props[name] = result[name]
        item = self.create(props, lazy_props)
        item.saved = True
        return item

    def _many_to_one_lazy_loader(self, prop, value):
        """Creates a lazy loader for many_to_ones properties.

        For one to many properties, the loader is built by the base access
        point directly.

        """
        def loader():
            """Wrapper function opening remote item when called."""
            # TODO: manage multiple identity properties
            condition = Condition(
                prop.remote_ap.identity_properties[0].name, "=", value)
            return (prop.remote_ap.open(condition), )
        return loader
                
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
        return self.to_alchemy_condition(And(* [Condition(prop.name, "=",
            item[prop.name]) 
            for prop in self.identity_properties]))
        
    def __transform_to_table(self, item):
        """Transform an item to a dict so that it can be saved."""
        item_dict = {}
        for name, prop in self.properties.items():
            if prop.auto:
                pass
            elif prop.relation == 'one-to-many':
                pass
            elif prop.relation == 'many-to-one':
                if item[name] is not None:
                    item_dict[name] = item[name].identity.conditions.values()[0]
                else:
                    item_dict[name] = None
            else:
                item_dict[name] = item[name]
        return item_dict

    def save(self, item):
        connection = self._table.bind.connect()
        transaction = connection.begin()
        value = self.__transform_to_table(item)
        try:
            statement = self._table.insert().values(value).execute()
            for (gen_id, id_prop) in zip(statement.inserted_primary_key, 
                    self.identity_properties):
                item[id_prop.name] = gen_id 
            transaction.commit()
        except:
            try:
                whereclause = self.__to_pk_where_clause(item)
                update = self._table.update()
                rows = update.where(whereclause).values(value).execute()
                if rows.rowcount == 0:
                    raise
                transaction.commit()
            except:
                transaction.rollback()
                raise
        finally:
            connection.close()
        item.saved = True

    def delete(self, item):
        whereclause = self.__to_pk_where_clause(item)
        self._table.delete().where(whereclause).execute()

    def view(self, kalamar_query):
        alchemy_query = sqlalchemy.sql.expression.select(from_obj = self._table)
        can, cants = kalamar_query.alchemy_validate(self, self.properties)
        if can:
            alchemy_query = can.to_alchemy(alchemy_query, self, 
                self.properties)
            if not alchemy_query.c:
                for name, prop in self.properties.items():
                    alchemy_query.append_column(prop.column.label(name))
            result = alchemy_query.execute()
        else:
            result = self.search(And())
        result = (dict(line) for line in result)
        result = list(result)
        cants = cants or QueryChain([])
        return cants(result)
