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

from ...query import QueryChain, QueryDistinct, QueryFilter, QueryOrder, \
    QueryRange, QuerySelect, QueryAggregate

from sqlalchemy.sql import expression
from ...request import _AndOr, Condition, And, Or, Not, RequestProperty

from ...item import AbstractItem

class QueryHaving(QueryFilter):
    pass

# Monky-patchers are allowed to skip some arguments
# pylint: disable=W0613

def query_chain_validator(self, access_point, properties):
    """Monkey-patched method on QueryChain to validate properties management.

    This function split the queries between what can be managed within sql
    alchemy and what can't.

    A QueryChain can be managed by SQLAlchemy if every subquery can be managed.
    Otherwise, it is (for now) considered unmanageable

    """
    last_seen = None
    orders = [None, QuerySelect, QueryHaving, QueryFilter, QueryDistinct, QueryOrder, QueryAggregate, QueryRange]
    for index, sub_query in enumerate(self.queries):
        splitted_queries = [QueryChain(sublist) if sublist else None
                for sublist in (self.queries[:index], self.queries[index:])]
        if sub_query.__class__ in orders:
            # Specific case: a "Filter" following an "Aggregate" should
            # result in a 'having' clause
            if sub_query.__class__ == QueryFilter and last_seen == QueryAggregate:
                self.queries[index] = sub_query = QueryHaving(sub_query.condition)
            elif orders.index(sub_query.__class__) < orders.index(last_seen):
                access_point.site.logger.warning("In SQL : %s, In python: %s" %
                        (splitted_queries[0], splitted_queries[1]))
                return splitted_queries
            last_seen = sub_query.__class__
        else:
            access_point.site.logger.warning("In SQL : %s, In python: %s" %
                        (splitted_queries[0], splitted_queries[1]))
            return splitted_queries
        managed, not_managed = sub_query.alchemy_validate(
            access_point, properties)
        if not_managed is not None:
            access_point.site.logger.warning("In SQL : %s, In python: %s" %
                    (splitted_queries[0], splitted_queries[1]))
            return splitted_queries
        properties = sub_query.validate(properties)
    return self, None


def query_chain_to_alchemy(self, access_point, tree, alchemy_query):
    for query in self.queries:
        alchemy_query = query.to_alchemy(access_point, tree, alchemy_query)
    return alchemy_query

def standard_validator(self, access_point, properties):
    """Validator for query types which can always be managed by SQLAlchemy."""
    return self, None


def query_filter_validator(self, access_point, properties):
    """Monkey-patched method on QueryFilter checking properties management.

    A query filter can be managed from within alchemy if every property which
    must be tested belongs to an alchemy access point.

    """
    from . import Alchemy

    cond_tree = self.condition.properties_tree
    def check_operators(condition):
        if isinstance(condition, (_AndOr, Not)):
            return all(check_operators(c) for c in condition.sub_requests)
        else:
            return condition.operator in access_point.dialect.SUPPORTED_OPERATORS

    def inner_manage(name, values, properties):
        """Recursive method to find wether a property can be managed from
        sqlalchemy"""
        if name not in properties:
            return False
        elif not isinstance(values, dict):
            return True
        elif properties[name].remote_ap:
            remote_ap = properties[name].remote_ap
            if isinstance(remote_ap, Alchemy) \
                    and access_point.url == remote_ap.url:
                return all(inner_manage(new_name, values, remote_ap.properties)
                           for new_name, values in values.items())
            else:
                return False
    if check_operators(self.condition) and \
        all(inner_manage(name, values, properties)
           for name, values in cond_tree.items()):
        return self, None
    else:
        return None, self

def query_filter_to_alchemy(self, access_point, tree, alchemy_query):
    alchemy_query.append_whereclause(_build_where(access_point, self.condition, tree))
    return alchemy_query

def query_select_validator(self, access_point, properties):
    """Validate that the query select can be managed from SQLAlchemy.

    A query select can be managed if the properties it aliases all belong to an
    Alchemy access point instance.

    """
    from . import Alchemy

    def isvalid(select, properties):
        """Check if ``select`` is valid according to ``properties``."""
        for value in select.mapping.values():
            if not access_point.dialect.supports(value, properties):
                access_point.site.logger.warning(
                    'Access point does not support %s' % value)
                return False
        for prop, sub_select in select.sub_selects.items():
            remote_ap = prop.return_property(properties).remote_ap
            if not (isinstance(remote_ap, Alchemy) and
                    remote_ap.url == access_point.url and
                    isvalid(sub_select, remote_ap.properties)):
                # We need further tests: if we can after all, let's do it
                return False
        return True

    if isvalid(self, properties):
        return self, None
    else:
        return None, self

def query_select_to_alchemy(self, access_point, tree, alchemy_query):
    return alchemy_query

def query_aggregate_validator(self, access_point, properties):
    # Assume its valid
    return self, None

def query_aggregate_to_alchemy(self, access_point, tree, alchemy_query):
    new_columns = []
    old_columns = dict((elem.name, elem) for elem in alchemy_query._raw_columns)
    groupers = [ col for name, col in old_columns.items()
            if name in (group.name for group in self.groupers)]
    alchemy_query = alchemy_query.group_by(*groupers)
    for grouper in self.groupers:
        # Keep grouper columns
        new_columns.append(old_columns.get(grouper.name, grouper.name))
    for key, value in self.aggregates.items():
        gen_agg = access_point.dialect.SUPPORTED_AGGREGATES[value.__class__]
        new_columns.append(gen_agg(old_columns, key, value))
    return alchemy_query.with_only_columns(new_columns)


def _make_condition(condition, column):
    value = condition.value
    if isinstance(value, AbstractItem):
        # TODO: manage multiple foreign key
        value = value.reference_repr()
    if condition.operator == "=":
        return column == value
    elif condition.operator == "!=":
        return column != value
    else:
        return column.op(condition.operator)(value)



def _build_where(access_point, condition, tree):
    if isinstance(condition, (And, Or, Not)):
        alchemy_conditions = tuple(
            _build_where(access_point, sub_condition, tree)
            for sub_condition in condition.sub_requests)
        return condition.alchemy_function(alchemy_conditions)
    else:
        prop = condition.property
        selectable = access_point.dialect.get_selectable(prop, tree)
        return _make_condition(condition, selectable)


def query_range_to_alchemy(self, access_point, tree, alchemy_query):
    if self.range.start:
        alchemy_query = alchemy_query.offset(self.range.start)
    if self.range.stop:
        alchemy_query = alchemy_query.limit(
            self.range.stop - (self.range.start or 0))
    return alchemy_query

def query_order_to_alchemy(self, access_point, tree, alchemy_query):
    for key, reverse in self.orderbys:
        alchemy_query = alchemy_query.order_by(
            expression.asc(key) if reverse else expression.desc(key))
    return alchemy_query

def query_having_to_alchemy(self, access_point, tree, alchemy_query):
    selectable = alchemy_query.c[self.condition.property.name].proxies[0]
    return alchemy_query.having(_make_condition(self.condition, selectable))

def query_distinct_to_alchemy(self, access_point, tree, alchemy_query):
    return alchemy_query.distinct()


QueryChain.alchemy_validate = query_chain_validator
QueryDistinct.alchemy_validate = standard_validator
QueryFilter.alchemy_validate = query_filter_validator
QueryOrder.alchemy_validate = standard_validator
QueryRange.alchemy_validate = standard_validator
QuerySelect.alchemy_validate = query_select_validator
QueryAggregate.alchemy_validate = query_aggregate_validator


QueryChain.to_alchemy = query_chain_to_alchemy
QueryFilter.to_alchemy = query_filter_to_alchemy
QuerySelect.to_alchemy = query_select_to_alchemy
QueryOrder.to_alchemy = query_order_to_alchemy
QueryRange.to_alchemy = query_range_to_alchemy
QueryHaving.to_alchemy = query_having_to_alchemy
QueryDistinct.to_alchemy = query_distinct_to_alchemy
QueryAggregate.to_alchemy = query_aggregate_to_alchemy

# pylint: enable=W0613
