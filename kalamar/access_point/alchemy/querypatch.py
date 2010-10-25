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
Query helpers for the Alchemy access point.

"""

from ... import query
from ...request import And, Or, Not

from sqlalchemy.sql import expression


def query_chain_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey-patched method on QueryChain to convert to alchemy."""
    for sub_query in self.queries:
        alchemy_query = sub_query.to_alchemy(
            alchemy_query, access_point, properties)
        properties = sub_query.validate(access_point.site, properties)
    return alchemy_query


def query_chain_validator(self, access_point, properties):
    """Monkey patched method on QueryChain to validate what can be managed
    within sql alchemy and what can't.
    
    A QueryChain can be managed by sqlalchemy if every subquery can be managed.
    Otherwise, it is (for now) considered unmanageable
    """
    cans = []
    for sub_query in self.queries:
        managed, not_managed = sub_query.alchemy_validate(
            access_point, properties)
        if not_managed is not None:
            return None, self
        if managed is not None:
            cans.append(managed)
        properties = sub_query.validate(access_point.site, properties)
    #TODO  : proper cans & cants management
    query_can = query.QueryChain(cans) 
    return query_can, None

def standard_validator(self, access_point, properties):
    """Default validator for query types which can always be managed by
    sqlalchemy"""
    return self, None


def query_filter_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey-patched method on QueryFilter to convert to alchemy."""
    access_points = access_point.site.access_points

    def to_alchemy_condition(condition):
        """Converts a kalamar condition to an sqlalchemy condition."""
        if isinstance(condition, (And, Or, Not)):
            alchemy_conditions = tuple(
                to_alchemy_condition(sub_condition)
                for sub_condition in condition.sub_requests)
            return condition.alchemy_function(alchemy_conditions)
        else:
            column = properties[condition.property.name].column
            if condition.operator == "=":
                return column == condition.value
            else:
                return column.op(condition.operator)(condition.value)

    def build_join(tree, properties, alchemy_query):
        """Builds the necessary joins for a condition"""
        for name, values in tree.items():
            prop = properties[name]
            if prop.remote_ap:
                remote_ap = access_points[prop.remote_ap]
                join_col1 = alchemy_query.corresponding_column(prop.column)
                if prop.relation == "many-to-one":
                    join_col2 = alchemy_query.corresponding_column(
                        remote_ap.properties[remote_ap.identity_properties[0]])
                    alchemy_query = alchemy_query.join(remote_ap._table,
                            onclause = join_col1 == join_col2)
                    alchemy_query = build_join(values, 
                            remote_ap.properties, alchemy_query)
        return alchemy_query
    alchemy_query = build_join(
        self.condition.properties_tree, properties, alchemy_query)
    alchemy_query = alchemy_query.where(to_alchemy_condition(self.condition))
    return alchemy_query


def query_filter_validator(self, access_point, properties):
    """Monkey-patched method on QueryFilter to assert the query can be managed
    from within sqlalchemy.

    A query filter can be managed from within alchemy if every property which
    must be tested belongs to an alchemy access point
    """
    from . import Alchemy
    cond_tree = self.condition.properties_tree
    access_points = access_point.site.access_points

    def inner_manage(name, values, properties):
        """Recursive method to find wether a property can be managed from
        sqlalchemey"""
        if name not in properties:
            return False
        elif properties[name].remote_ap:
            remote_ap = access_points[properties[name].remote_ap]
            return isinstance(remote_ap, Alchemy) and \
                all(inner_manage(name, values, remote_ap.properties)
                    for name, values in cond_tree.items())
        else: 
            return True

    if all(inner_manage(name, values, properties)
           for name, values in cond_tree.items()):
        return self, None
    else:
        return None, self


def query_select_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey-patched method on QuerySelect to convert to alchemy.
    
    First, the mapping and sub selects are walked to build a join with other
    access points.

    Then, they are walked a second time to find what should be added to the
    SELECT clause
    """
    access_points = access_point.site.access_points
    def build_join(select, properties, join):
        """Walks the mapping to build the joins"""
        
        for name, sub_select in select.sub_selects.items():
            remote_ap = access_points[properties[name].remote_ap]
            remote_property_name = properties[name].remote_property
            remote_property = remote_ap.properties[remote_property_name]
            col1 = properties[name].column
            col2 = remote_property.column
            join = join.outerjoin(remote_ap._table, 
                onclause = col1 == col2)
            join = build_join(sub_select, remote_ap.properties, join)
        return join
    def build_select(select, properties, alchemy_query):
        """Walks the mapping to append column"""
        for name, value in select.mapping.items():
            if value.name == u'*':
                for prop_name, prop in properties.items():
                    column = prop.column
                    if prop.relation is None:
                        if name is not u'':
                            name = prop_name
                        else:
                            name = "_".join([name,
                            prop_name])
                        alchemy_query.append_column(column.label(name))
            else:
                column = properties[value.name].column
                alchemy_query.append_column(column.label(name))
        for name, sub_select in select.sub_selects.items():
            remote_ap = access_points[properties[name].remote_ap]
            alchemy_query = build_select(sub_select, remote_ap.properties,
                    alchemy_query)
        return alchemy_query
    join = build_join(self, properties, access_point._table)
    alchemy_query = alchemy_query.select_from(join)
    build_select(self, properties, alchemy_query)
    return alchemy_query


def query_select_validator(self, access_point, properties):
    """Validates that the query select can be managed from sqlalchemy
    
    A query select can be managed if the properties it aliases all belong to an
    Alchemy access point instance
    
    """
    from . import Alchemy
    access_points = access_point.site.access_points

    def isvalid(select, properties):
        for name, sub_select in select.sub_selects.items():
            remote_ap = access_points[properties[name].remote_ap]
            if not isinstance(remote_ap, Alchemy) or \
                    not isvalid(sub_select, remote_ap.properties):
                return False
        return True

    if isvalid(self, properties):
        return self, None
    else:
        return None, self


def query_distinct_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey patched method converting a QueryDistinct to an sqlalchemy
    query"""
    return alchemy_query.distinct()
    

def query_range_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey patched method converting a QueryRange to an sqlalchemy query"""
    if self.range.start:
        alchemy_query = alchemy_query.offset(self.range.start)
    if self.range.stop:
        alchemy_query = alchemy_query.limit(
            self.range.stop - (self.range.start or 0))
    return alchemy_query


def query_order_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey patched method converting a QueryOrder to an sqlalchemy query"""
    for key, order in self.orderbys:
        alchemy_query = alchemy_query.order_by(
            expression.asc(key) if order else expression.desc(key))
    return alchemy_query


query.QueryChain.alchemy_validate = query_chain_validator
query.QueryDistinct.alchemy_validate = standard_validator
query.QueryFilter.alchemy_validate = query_filter_validator
query.QueryOrder.alchemy_validate = standard_validator
query.QueryRange.alchemy_validate = standard_validator
query.QuerySelect.alchemy_validate = query_select_validator

query.QueryChain.to_alchemy = query_chain_to_alchemy
query.QueryDistinct.to_alchemy = query_distinct_to_alchemy
query.QueryFilter.to_alchemy = query_filter_to_alchemy
query.QueryOrder.to_alchemy = query_order_to_alchemy
query.QueryRange.to_alchemy = query_range_to_alchemy
query.QuerySelect.to_alchemy = query_select_to_alchemy
