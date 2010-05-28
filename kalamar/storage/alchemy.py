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
 string_type = {"sql-type" : "character varying",
                "alchemy-type" : String}
 integer_type = {"sql-type" : "integer",
                "alchemy-type" : Integer}
 date_type = {"sql-type" : "date",
              "alchemy-type" : Date}
 id_type = {"sql-type" : "serial"
              ,"alchemy-type" : Integer
              ,"foreign-type" : "integer"}
 types = {"string" : String,
          "date"   : Date,
           "id"    : Integer}


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
        super(AlchemyAccessPoint, self).__init__(config)
        metadata  = self.get_metadata()
        self.pks = []
        table_name = self.config.url.split('?')[1]
        self.columns = {} 
        for name,props in config.additional_properties['properties'].items():
            alchemy_type = SqlAlchemyTypes.types.get(props.get('type',None),None)
            column_name = props.get('dbcolumn',name)
            if props.get("is_primary",None) == "true":
                self.pks.append(str(name))
                column = Column(column_name, alchemy_type, primary_key = True)
            elif props.get("foreign-key",None):
                column = Column(column_name,alchemy_type,ForeignKey(props.get("foreign-key")))
            else:
                column = Column(column_name,alchemy_type)
            self.columns[name]=column
        self.table = Table(table_name,metadata,*self.columns.values())
    
    def _convert_item_to_table_dict(self, item, ffunction = lambda x,y: x and y):
        temp = {} 
        for name in self.get_storage_properties():
            if ffunction(name,item[name]):
                temp[self.columns[name].name] = item[name]
        return temp
    
    def _extract_primary_key_value(self,item):
        return self._convert_item_to_table_dict(item,ffunction = (lambda name,value: name in self.pks))

    def _process_conditions(self,conditions):
        return [self.table.c[cond.property_name].op(cond.operator)(cond.value) for cond in conditions]
    
    def _values_to_where_clause(self,properties):
        return sql_and(*[(self.table.c[prop] == value) for prop,value in properties.items()])
    
    def _where_clause_from_pk(self,item):
        return self._values_to_where_clause(self._extract_primary_key_value(item)) 

    def _storage_search(self, conditions):
        """Return a sequence of tuple (properties, file_opener)"""
        conds = sql_and(self._process_conditions(conditions))
        select = self.table.select()
        for cond in conds:
            select.append_whereclause(cond)
        result = select.execute()
        for line in result:
            yield (line.items(),"")

    def get_storage_properties(self):
        """Return the list of properties used by the storage (not aliased).

        This method has to be overriden.

        """
        return self.columns.keys()
    
    

    def save(self, item):
        """Update or add the item.

        This method has to be overriden.

        """
        kwargs = self._convert_item_to_table_dict(item)
        transaction = self.table.bind.connect()
        trans = transaction.begin()
        try:
            ids = self.table.insert().values(**kwargs).execute().inserted_primary_key
            for (id, pk) in zip(ids, self.pks):
                item[pk] = id
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
        
        This property has to be overriden.

        """
        return self.pks

