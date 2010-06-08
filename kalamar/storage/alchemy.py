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

"""
SqlAlchemy access point.

This access point relies on SqlAlchemy to manage any RDBMS 
as a kalamar storage back-end
"""


from kalamar import utils
from kalamar.item import Item
from kalamar import parser
from kalamar.storage.base import AccessPoint
from sqlalchemy import Table, Column, MetaData, ForeignKey, create_engine
from sqlalchemy import String,Integer,Date
from sqlalchemy import and_ as sql_and

class SqlAlchemyTypes:
 types = {"string" : String,
          "date"   : Date,
          "id"    : Integer,
          "integer" : Integer}


class AlchemyAccessPoint(AccessPoint):
   

    metadatas = {}
    
    sql_operators = {
        utils.operator.eq : '=',
        utils.operator.ne : '!=',
        utils.operator.gt : '>',
        utils.operator.ge : '>=',
        utils.operator.lt : '<',
        utils.operator.le : '<=',
        utils.re_match : '~',
        utils.re_not_match : '!~'}


    def get_metadata(self):
        """Returns a sql-alchemy metadata, associated with an engine
        
        """
        url = self.config.url.split('?')[0].split('-')[1] 
        #TODO: ensure this is thread-safe  
        metadata = AlchemyAccessPoint.metadatas.get(url,None)
        if not metadata:
            #Constructs an engine using the url, stripping out the alchemy- part
            engine = create_engine(url)
            metadata = MetaData()
            metadata.bind = engine
            AlchemyAccessPoint.metadatas[url] = metadata
        return metadata 


    def __init__(self, config):
        """ Init the accesspoint with the table definitions contained in the config.a

        """
        super(AlchemyAccessPoint, self).__init__(config)
        metadata  = self.get_metadata()
        self.pks = []
        self.db_mapping = {}
        table_name = self.config.url.split('?')[1]
        self.columns = {} 
        self.prop_names = []
        self.remote_props = {}
        for name, props in config.properties.items() :
            alchemy_type = SqlAlchemyTypes.types.get(props.get('type',None),None)
            column_name = props.get('dbcolumn',name)
            self.prop_names.append(name)
            if 'foreign_ap' in props :
                self.remote_props[name] = props['foreign_ap' ]
            if not column_name == name :
                self.db_mapping[column_name] = name
            if props.get("is_primary",None) == "true":
                self.pks.append(str(name))
                column = Column(column_name, alchemy_type, primary_key = True, key = name)
            elif props.get("foreign-key",None):
                column = Column(column_name,alchemy_type,ForeignKey(props.get("foreign-key")),key = name)
            else:
                column = Column(column_name,alchemy_type,key = name)
            self.columns[name]=column
        self.table = Table(table_name,metadata,*self.columns.values())
        self.prop_names += [name for name in self.storage_aliases]
    
    def _convert_item_to_table_dict(self, item, ffunction = lambda x,y: x and y):
        """ Convert a kalamar item object to a dictionary for sqlalchemy.
            
            Optionnaly, a function can be provided to filter the item properties.
        """
        temp = {} 
        for name in self.get_storage_properties() :
            if ffunction(name,item[name]):
                if name in self.remote_properties:
                    if item.is_loaded(name):
                        remote_ap = self.site.access_points[self.remote_properties[name]]
                        remote_pk = remote_ap.primary_keys
                        #TODO : Consider the case when a remote_ap has more than one primary key.
                        temp[name] = item[name][remote_pk[0]]
                    else:
                        temp[name] = item[name]
                else:
                    temp[name] = item[name]
        return temp
    
    def _extract_primary_key_value(self,item):
        """ Extract a dictionary containing the primary key values from an item.

        """
        return self._convert_item_to_table_dict(item,ffunction = (lambda name,value: name in self.pks))

    def _process_conditions(self,conditions):
        """ Extract sqlalchemy expressions from a Condition iterable.

        """
        return [self.columns[cond.property_name].op(AlchemyAccessPoint.sql_operators[cond.operator])(cond.value) for cond in conditions]
    
    def _values_to_where_clause(self,properties):
        """ From a dictionary, constructs sqlalchemy 'where' expression.

        """
        return sql_and(*[(self.columns[prop] == value) for prop,value in properties.items()])
    
    def _where_clause_from_pk(self,item):
        """From an item, constructs an sqlalchemy 'where' expression on its pks.

        """
        return self._values_to_where_clause(self._extract_primary_key_value(item)) 

    def _transform_aliased_properties(self,line):
        return dict([(self.db_mapping.get(name,name),value) for name,value in line.items()])

    def _storage_search(self, conditions):
        """Return a sequence of tuple (properties, file_opener)"""
        conds = sql_and(self._process_conditions(conditions))
        select = self.table.select()
        for cond in conds:
            select.append_whereclause(cond)
        result = select.execute()
        for line in result:
            yield (self._transform_aliased_properties(line),"",self.remote_properties)

    def get_storage_properties(self):
        """Return the list of properties used by the storage (not aliased).

        """
        return self.columns.keys()
   


    def load(self,property_name, item, ref):
        if property_name in self.remote_properties:
            return self.site.open(self.remote_properties[property_name], [ref])
    
    def save(self, item):
        """Update or add the item.

        This method has to be overriden.

        """
        kwargs = self._convert_item_to_table_dict(item)
        conn = self.table.bind.connect()
        trans = conn.begin()
        try:
            ids = self.table.insert().values(**kwargs).execute().inserted_primary_key
            for (id, pk) in zip(ids, self.pks):
                item[pk] = id
            trans.commit()
            return item
        except :
            try:
                kwargs = self._convert_item_to_table_dict(item,ffunction = lambda name,value : name not in self.pks)
                whereclause = self._where_clause_from_pk(item)
                update = self.table.update()
                update.where(whereclause).values(**kwargs).execute()
                trans.commit()
            except:
                trans.rollback()
        finally:
            conn.close()
            

    def remove(self, item):
        """Remove/delete the item from the backend storage.

        This method has to be overriden.

        """
        whereclause = self._where_clause_from_pk(item)
        self.table.delete().where(whereclause).execute()
   
    @property
    def primary_keys(self):
        """List of primary keys names.
        
        Here, "primary key" must be understood as "a sufficient set of keys to
        make a request returning 0 or 1 object".

        This list must be ordered and stable for a given access point, in order
        to construct canonical requests for items.
        
        """
        return self.pks

    @property
    def remote_properties(self):
        return self.remote_props
    
    @property
    def property_names(self):
        return self.prop_names

