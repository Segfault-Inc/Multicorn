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
from sqlalchemy.sql.expression import alias
from sqlalchemy import String,Integer,Date,Numeric,DateTime
from sqlalchemy import and_ as sql_and

class SqlAlchemyTypes:
 types = {"string" : String,
          "date"   : Date,
          "id"    : Integer,
          "integer" : Integer,
          "decimal" : Numeric,
          "datetime" : DateTime
         }


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
            engine = create_engine(url, echo=True)
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
        self.property_names = []
        self.remote_props = {}
        for name, props in config.properties.items() :
            if 'foreign-ap' in props   :
                self.remote_props[name] = props['foreign-ap']
            if props.get('relation-type',None) != 'one-to-many':
                alchemy_type = SqlAlchemyTypes.types.get(props.get('type',None),None)
                column_name = props.get('dbcolumn',name)
                self.property_names.append(name)
                ispk = False
                if not column_name == name :
                    self.db_mapping[column_name] = name
                if props.get("is_primary",None) == "true":
                    self.pks.append(str(name))
                    ispk = True
                if props.get("foreign-key",None):
                    column = Column(column_name,alchemy_type, ForeignKey(props.get("foreign-key")),key = name,primary_key=ispk)
                else:
                    column = Column(column_name,alchemy_type,key = name,primary_key=ispk)
                self.columns[name] = column
        self.table = Table(table_name,metadata,*self.columns.values())
    
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
        return [self._get_remote_column(cond.property_name).op(AlchemyAccessPoint.sql_operators.get(cond.operator,"="))(cond.value) for cond in conditions]
    
    def _values_to_where_clause(self,properties):
        """ From a dictionary, constructs sqlalchemy 'where' expression.

        """
        return sql_and(*[(self.columns[prop] == value) for prop,value in properties.items()])
    
    def _where_clause_from_pk(self,item):
        """From an item, constructs an sqlalchemy 'where' expression on its pks.

        """
        return self._values_to_where_clause(self._extract_primary_key_value(item)) 

    def _transform_aliased_properties(self, line):
        return dict([(self.db_mapping.get(name,name),value) for name,value in line.items()])

    def _extract_condition_propert_(self, condition):
        return 

    def _get_remote_column(self, compound_property):
        splitted = compound_property.split(".")
        if len(splitted) == 1:
            return self.columns[compound_property]
        else:
            remote_ap_name = self.remote_props[splitted[0]]
            return self.site.access_points[remote_ap_name]._get_remote_column(str.join(".",splitted[1:]))

    def _process_mapping(self, mapping):
        not_managed_mapping = {}
        managed_mapping = {}
        for key,value in mapping.items():
            splitted = value.split(".")
            if len(splitted) == 1:
                managed_mapping[key] = value
            else:
                prop = splitted[0]
                if prop in self.remote_properties:
                    remote_ap = self.remote_properties[prop]
                    if remote_ap in not_managed_mapping:
                        not_managed_mapping[remote_ap].update({key:".".join(splitted[1:])})
                    else:
                        not_managed_mapping[remote_ap]={key: ".".join(splitted[1:] )}
                else:
                    managed_mapping[key] = value
        return managed_mapping,not_managed_mapping
            

    def _build_join(self,mapping,conditions,join=None):
        if join == None:
            join = self.table
        mapfn = lambda x : x.split(".")[0]
        filterfn = lambda x : x in self.remote_props
        extract_condition_prop = lambda x : x.property_name
        managed_mapping,not_managed_mapping = self._process_mapping(mapping)
        managed_conditions, not_managed_conditions = self._process_mapping_conditions(conditions)
        print not_managed_mapping
        for ap in not_managed_mapping :
            remote_ap = self.site.access_points[ap]
            join = join.join(remote_ap.table)
        for ap in not_managed_conditions :
            remote_ap = self.site.access_points[ap]
            join = join.join(remote_ap.table)
        for ap in not_managed_mapping :
            remote_ap = self.site.access_points[ap]
            remote_mapping =  not_managed_mapping[ap]
            print remote_mapping 
            print "About to join " + self.name + " WITH " + ap
            join = remote_ap._build_join(remote_mapping, not_managed_conditions.get(ap, []),join)
        for ap in dict(filter(lambda x: x[0] not in mapping, not_managed_conditions.items())):
            remote_ap = self.site.access_points[ap]
            join = remote_ap._build_join({},  not_managed_conditions.get(ap, []),join)
        return join

    def view(self, mapping, conditions,joins={}):
        conds = sql_and(self._process_conditions(conditions))
        select = self.table.select(None,from_obj=self._build_join(mapping,conditions))
        for cond in conds:
            select.append_whereclause(cond)
        select = select.with_only_columns([self._get_remote_column(value).label(key) for key,value in mapping.items()])
        for line in select.execute():
            yield self._transform_aliased_properties(line)


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
l
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
    
    def get_properties(self):
        return self.property_names

