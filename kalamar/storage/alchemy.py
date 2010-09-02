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
from sqlalchemy.sql.expression import alias,Select
from sqlalchemy import String,Integer,Date,Numeric,DateTime,Boolean,Unicode
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or

class SqlAlchemyTypes:
 types = {"string" : Unicode,
          "date"   : Date,
          "id"    : Integer,
          "integer" : Integer,
          "decimal" : Numeric,
          "boolean" : Boolean,
          "datetime" : DateTime,
          "password" : String,
          "email"    : String
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
        echo = self.config.debug
        #TODO: ensure this is thread-safe  
        metadata = AlchemyAccessPoint.metadatas.get(url,None)
        if not metadata:
            #Constructs an engine using the url, stripping out the alchemy- part
            engine = create_engine(url, echo=echo)
            metadata = MetaData()
            metadata.bind = engine
            AlchemyAccessPoint.metadatas[url] = metadata
        return metadata 


    def _make_column_from_property(self, name, props):
        if 'foreign-ap' in props   :
            self.remote_props[name] = props['foreign-ap']
        try :
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
            else :
                self.one_to_manies[name] = props.get('foreign-property',self.name)
        except: 
            print "A problem occured while creating config for ap %s with column %s " % (self.name, name)










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
        self.config = config
        self.parent_ap = config.additional_properties.get('inherits',None)
        self.one_to_manies = {}
        self.typed_properties = {}
        for name, props in config.properties.items() :
            self._make_column_from_property(name,props)
            self.typed_properties[name] = props.get('type','remote')
        if self.parent_ap :
            parent_props = dict(self._get_parent_ap().config.properties)
            for name, props in parent_props.items() :
                self.typed_properties[name] = props.get('type','remote')
                if name not in self.config.properties:
                    self._make_column_from_property(name, props)
            parent_props.update(self.config.properties)
            self.config.properties = parent_props
        self.table = Table(table_name,metadata,*self.columns.values())
   

    def _convert_item_to_table_dict(self, item, ffunction = lambda x,y: x and not (y == None)):
        """ Convert a kalamar item object to a dictionary for sqlalchemy.
            
            Optionnaly, a function can be provided to filter the item properties.
        """
        temp = {} 
        for name in self.get_storage_properties() :
            if ffunction(name,item[name]):
                if name in self.remote_properties:
                    temp[name] = item[name]
                    if item.is_loaded(name) and isinstance(item[name],Item):
                        remote_ap = self.site.access_points[self.remote_properties[name]]
                        remote_pk = remote_ap.primary_keys
                        #TODO : Consider the case when a remote_ap has more than one primary key.
                        if item[name]:
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
        sql_conds = [] 
        for cond in conditions:
            if isinstance(cond.value,list):
                op = " in "
                value = "( " + " , ".join(map(str,cond.value)) + " ) " 
            else :
                op = AlchemyAccessPoint.sql_operators.get(cond.operator,"=")
                value = cond.value

            if op == '=':
                sql_conds.append(self._get_remote_column(cond.property_name) == value)
            else:
                sql_conds.append(self._get_remote_column(cond.property_name).op(op)(value))
        return sql_conds
    
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

    def _get_parent_ap(self):
        return self.site.access_points.get(self.parent_ap,None)

    def _get_remote_column(self, compound_property):
        splitted = compound_property.split(".")
        if len(splitted) == 1:
            if compound_property in self.columns: 
                return self.columns.get(compound_property,None)
            elif compound_property in self.one_to_manies:
                remote_ap_name = self.remote_props[splitted[0]]
                return self.site.access_points[remote_ap_name].columns[self.one_to_manies[compound_property]]
            return self._get_parent_ap()._get_remote_column(compound_property)
        else:
            remote_ap_name = self.remote_props[splitted[0]]
            return self.site.access_points[remote_ap_name]._get_remote_column(str.join(".",splitted[1:]))

    def _make_condition(self, cond, relative_path = None):
        relative_path = self.table if relative_path == None else relative_path
        if isinstance(cond.value,list):
            op = " in "
            value = "( " + " , ".join(map(str,cond.value)) + " ) " 
        else :
            op = AlchemyAccessPoint.sql_operators.get(cond.operator,"=")
            value = cond.value
        if op == '=':
            return relative_path.corresponding_column(self._get_remote_column(cond.property_name)) == value
        else:
            return relative_path.corresponding_column(self._get_remote_column(cond.property_name)).op(op)(value)



    def _extract_joins(self, properties_map, conditions, current_select, relative_path=None, order_by = {} ):
        relative_path = self.table if relative_path == None else relative_path
        joins = []
        not_managed = {}
        not_managed_conditions = {}
        sql_conditions = {}
        orders = []
        for property_name, property_path in properties_map.items():
            splitted = property_path.split(".")
            property_root = splitted[0].replace("<",'')
            if len(splitted) == 1:
                selected_column = relative_path.corresponding_column(self.columns[property_root])
                selected_column = selected_column.label(property_name)
                if property_name in order_by:
                    if order_by[property_name] == 'asc':
                        orders.append(selected_column.asc())
                    else :
                        orders.append(selected_column.desc())
                current_select.append_column(selected_column)
            else :
                if property_root in self.remote_properties:
                    remote_ap = self.remote_properties[property_root]
                    remote_not_managed_props = not_managed.get(splitted[0],{})
                    remote_not_managed_props[property_name] = ".".join(splitted[1:])
                    not_managed[splitted[0]] = remote_not_managed_props
        for cond in conditions :
            splitted = cond.property_name.split(".")
            if len(splitted) == 1:
                sql_cond = (self._make_condition(cond,relative_path))
                sql_conditions[cond] = sql_cond
            else: 
                property_root = splitted[0]
                if property_root in self.remote_properties:
                    remote_ap = self.remote_properties[property_root]
                    remote_not_managed_conds = not_managed_conditions.get(property_root,[])
                    cond.property_name = ".".join(splitted[1:])
                    remote_not_managed_conds.append(cond)
                    not_managed_conditions[property_root] = remote_not_managed_conds
        for remote_prop_name, properties in not_managed.items():
            isOuter = remote_prop_name[0] == '<'
            remote_prop_name = remote_prop_name.replace('<','')
            remote_ap_name = self.remote_properties[remote_prop_name]
            remote_ap = self.site.access_points[remote_ap_name]
            remote_conditions = not_managed_conditions.pop(remote_prop_name, [])
            child_relative_path = remote_ap.table.alias()
            #relative_path = child_relative_path.join(relative_path,firstjoincol == secondjoincol)
            child_joins,child_conditions,child_orders = remote_ap._extract_joins(properties,
                    remote_conditions,current_select, child_relative_path, order_by=order_by)
            if remote_prop_name in self.one_to_manies:
                firstjoincol = child_relative_path.corresponding_column(self._get_remote_column(remote_prop_name))
                secondjoincol = relative_path.corresponding_column(firstjoincol.foreign_keys[0].column)
            else : 
                firstjoincol = relative_path.corresponding_column(self._get_remote_column(remote_prop_name))
                secondjoincol = child_relative_path.corresponding_column(firstjoincol.foreign_keys[0].column)
            if isOuter:
                relative_path = relative_path.outerjoin(child_joins[0],firstjoincol == secondjoincol)
            else : 
                relative_path = relative_path.join(child_joins[0],firstjoincol == secondjoincol)
            sql_conditions.update(child_conditions)
            orders.extend(child_orders)
        for remote_prop_name, conditions in not_managed_conditions.items():
            remote_ap_name = self.remote_properties[remote_prop_name]
            remote_ap = self.site.access_points[remote_ap_name]
            child_relative_path = remote_ap.table.alias()
            #relative_path = child_relative_path.join(relative_path,firstjoincol == secondjoincol)
            child_joins, child_conditions, child_orders = remote_ap._extract_joins({}, conditions,current_select
                    ,child_relative_path, order_by=order_by)
            if remote_prop_name in self.one_to_manies:
                firstjoincol = child_relative_path.corresponding_column(self._get_remote_column(remote_prop_name))
                secondjoincol = relative_path.corresponding_column(firstjoincol.foreign_keys[0].column)
            else : 
                firstjoincol = relative_path.corresponding_column(self._get_remote_column(remote_prop_name))
                secondjoincol = child_relative_path.corresponding_column(firstjoincol.foreign_keys[0].column)
            relative_path = relative_path.join(child_joins[0],firstjoincol == secondjoincol)
            sql_conditions.update(child_conditions)
            orders.extend(child_orders)
        joins = [relative_path]
        return joins,sql_conditions, orders
        

    def _flatten(self, conditions):
        for elem in conditions:
            if type(elem) in (tuple, list,utils.ConditionAnd,utils.ConditionOr):
                for i in self._flatten(elem):
                    yield i
            else:
                yield elem


    def view(self, mapping, conditions, **kwargs):
        query = Select(None,None,from_obj=self.table,use_labels=True)
        joins, sql_conditions, orders = self._extract_joins(mapping,self._flatten(conditions), query, order_by =
        kwargs.get('order_by',{}))
        for join in joins:
            query.append_from(join)
        if len(sql_conditions) > 0:    
            alchemconds = self._to_alchemy_conditions(conditions, sql_conditions)
            query.append_whereclause(alchemconds)
        query = apply(query.order_by, orders)
        if kwargs.get('distinct',False):
            query = query.distinct()
        if 'range' in kwargs :
            start = kwargs['range'][0]
            stop  = kwargs['range'][1] - start 
            query = query.offset(start).limit(stop)
        for line in query.execute():
            yield self._transform_aliased_properties(line)

    def _to_alchemy_conditions(self, conditions, sql_conditions):
        if isinstance(conditions,list):
            if len(conditions) == 1:
                return self._to_alchemy_conditions(conditions[0],sql_conditions)
            elif conditions.operator == "and":
                return apply(sql_and,[self._to_alchemy_conditions(cond, sql_conditions) for cond in conditions])
            elif conditions.operator == "or":
                return apply(sql_or,[self._to_alchemy_conditions(cond, sql_conditions) for cond in sql_conditions])
        else:
            return sql_conditions[conditions]



    def _storage_search(self, conditions):
        """Return a sequence of tuple (properties, file_opener)"""
        query = Select(None,None,from_obj=self.table,use_labels=True)
        mapping = dict([(name,col.name) for name,col in self.columns.items() ])
        joins, sql_conditions, orders = self._extract_joins(mapping,self._flatten(conditions), query)
        for join in joins:
            query.append_from(join)
        if len(sql_conditions) > 0:    
            alchemconds = self._to_alchemy_conditions(conditions, sql_conditions)
            query.append_whereclause(alchemconds)
        result = query.execute()
        for line in result:
            yield (self._transform_aliased_properties(line),"",self.remote_properties)

    def get_storage_properties(self):
        """Return the list of properties used by the storage (not aliased).
        """
        return self.columns.keys()
   


    def load(self,property_name, item, ref):
        if property_name in self.one_to_manies:
            remoteap = self.remote_properties[property_name]
            remote_prop = self.one_to_manies[property_name]
            return self.site.search(remoteap,{remote_prop : item[self.primary_keys[0]]})
        elif property_name in self.remote_properties:
            conds = []
            ref = filter(lambda x : x is not None, ref)
            if not ref:
                return None
            else:
                remoteap = self.remote_properties[property_name]
                for pk,ref in zip(self.site.access_points[remoteap].primary_keys,ref):
                    conds.append(utils.Condition(pk, "=",ref))
                return self.site.open(remoteap, conds)

    
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
                rp = update.where(whereclause).values(**kwargs).execute()
                if rp.rowcount == 0:
                    raise 
                trans.commit()
            except :
                trans.rollback()
                raise 
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
        return self.pks if len(self.pks) else self.site.access_points[self.parent_ap].primary_keys

    @property
    def remote_properties(self):
        return self.remote_props
    
    def get_properties(self):
        return self.property_names
    
    def get_typed_properties(self):
        return self.typed_properties 

