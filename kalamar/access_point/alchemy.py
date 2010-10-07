# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
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

from .base import AccessPoint
from ..item import Item
from ..property import Property 
from werkzeug import cached_property
from sqlalchemy import sql
from sqlalchemy import Table, Column, MetaData, ForeignKey, create_engine
from sqlalchemy.sql.expression import alias, Select
from sqlalchemy import String,Integer,Date,Numeric,DateTime,Boolean,Unicode
from ..request import Condition, And, Or, Not
from datetime import datetime, date

SQLALCHEMYTYPES = {
    unicode : Unicode,
    int : Integer,
    datetime : DateTime,
    date : Date
}

class AlchemyProperty(Property):

    def __init__(self, property_type, column_name, identity=False, auto=False,
                 default=None, mandatory=False, relation=None, remote_ap=None,
                 remote_property=None):
        super(AlchemyProperty, self).__init__(property_type, identity, auto, default, mandatory, 
                relation, remote_ap, remote_property)
        self.column_name = column_name
        self._column = None



class Alchemy(AccessPoint):

    __metadatas = {}

    def __init__(self, url, tablename, properties, identity_property, createtable=False):
        self.url = url
        self.properties = properties
        self.tablename = tablename
        self.identity_properties = [identity_property]
        self.createtable = createtable
        self.remote_alchemy_props = []
        for name, prop in self.properties.items():
            prop.name = name 
            if prop.relation is None:
                self._column_from_prop(prop)

    def _column_from_prop(self, prop):
        if prop._column is not None:
            return prop._column
        alchemy_type = SQLALCHEMYTYPES.get(prop.type,None)
        kwargs = {'key' : prop.name}
        if prop.name in self.identity_properties:
            kwargs['primary_key'] = True
        if prop.default:
            kwargs[default] = prop.default
        if prop.relation == 'many-to-one':
            foreign_ap = self.site.access_points[prop.remote_ap]
            prop.foreign_ap_obj = foreign_ap
            #Transpose the kalamar relation in alchemy if possible
            if isinstance(foreign_ap, Alchemy):
                foreign_table = foreign_ap.tablename
                #TODO: Fix this for circular dependencies
                foreign_column = self.__get_column("%s.%s" % (prop.name,
                    prop.remote_property))
                fk = ForeignKey("%s.%s" % (foreign_table,foreign_column),
                        use_alter = True, name = "%s_%s_fkey" %
                        (self.tablename, prop.name ))
                self.remote_alchemy_props.append(prop.name)
                alchemy_type = foreign_column.type
                column = Column(prop.column_name, alchemy_type, fk, **kwargs)
            else :
                foreign_prop = foreign_ap.properties[foreign_ap.identity_properties[0]]
                alchemy_type = alchemy_type or \
                    SQLALCHEMYTYPES.get(foreign_prop.type, None)
                column = Column(prop.column_name, alchemy_type, **kwargs)
        elif prop.relation == 'one-to-many':
            pass
        else :
            column = Column(prop.column_name, alchemy_type, **kwargs)
        prop._column = column
        return column


    @cached_property
    def _table(self):
        """ Initialize the sql alchemy engine on first access """
        metadata = Alchemy.__metadatas.get(self.url, None)
        if not metadata:
            engine = create_engine(self.url)
            metadata = MetaData()
            metadata.bind = engine
            Alchemy.__metadatas[self.url] = metadata
        self.metadata = metadata
        columns = [self._column_from_prop(prop) for prop in
                self.properties.values()]
        table = Table(self.tablename, metadata, *columns, useexisting=True)
        if self.createtable :
            table.create(checkfirst=True)
        return table
        

    def __get_column(self, propertyname):
        splitted = propertyname.split(".")
        prop = self.properties[splitted[0]]
        if len(splitted) > 1 :
            return prop.foreign_ap_obj.__get_column(".".join(splitted[1:]))
        else:
            return prop._column

    def __to_alchemy_condition(self, condition):
        if isinstance(condition, And):
            return apply(sql.and_,[self.__to_alchemy_condition(cond)
                for cond in condition.sub_requests])
        elif isinstance(condition, Or):
            return apply(sql.or_,[self.__to_alchemy_condition(cond)
                for cond in condition.sub_requests])
        elif isinstance(condition, Not):
            return apply(sql.not_,[self.__to_alchemy_condition(cond)
                for cond in condition.sub_requests])
        else:
            col = condition.property.kalamarProperty(self)._column
            if condition.operator == '=':
                return col == condition.value
            else:
                return col.op(condition.operator)(condition.value)
        
    def __item_from_result(self, result):
        lazy_props = {}
        props = {}
        for name, prop in self.properties.items():
            if prop.relation == 'one-to-many':
                lazy_props[name] = None
            elif prop.relation == 'many-to-one':
                lazy_props[name] = self._many_to_one_lazy_loader(prop, result[name])
            else: 
                props[name] = result[name]
        return self.create(props, lazy_props)

    def _many_to_one_lazy_loader(self, property, value):
        remote_ap = self.site.access_points[property.remote_ap]
        def loader():
            cond = Condition(remote_ap.identity_properties[0], "=", value)
            return (remote_ap.open(cond),)
        return loader
                
    def search(self, request):
        query = Select(None, None, from_obj=self._table, use_labels=True)
        query.append_whereclause(self.__to_alchemy_condition(request))
        for name, prop in self.properties.items():
            query.append_column(prop._column.label(name))
        result = query.execute()
        for line in result:
            yield self.__item_from_result(line)

    def __to_pk_where_clause(self, item):
        return self.__to_alchemy_condition(apply(And, [Condition(pk, "=", item[pk]) 
            for pk in self.identity_properties]))
        
    def __transform_to_table(self, item):
        item_dict = {}
        for prop, value in item.items():
            if self.properties[prop].relation == 'many-to-one':
                #TODO: more than one identity property
                item_dict[prop] = item[prop].identity.conditions.values()[0]
            elif self.properties[prop].relation == 'one-to-many':
                pass
            else :
                item_dict[prop] = value
        return item_dict


    def save(self, item):
        conn = self._table.bind.connect()
        trans = conn.begin()
        value = self.__transform_to_table(item)
        try:
            ids = self._table.insert().values(value).execute().inserted_primary_key
            for (id,pk) in zip(ids, self.identity_properties):
                item[pk] = id
            trans.commit()
        except:
            try:
                whereclause = self.__to_pk_where_clause(item)
                update = self._table.update()
                rp = update.where(whereclause).values(value).execute()
                if rp.rowcount == 0:
                    raise
                trans.commit()
            except:
                trans.rollback()
                raise
        finally:
            conn.close()

    def delete(self, item):
        whereclause = self.__to_pk_where_clause(item)
        self._table.delete().where(whereclause).execute()

 
