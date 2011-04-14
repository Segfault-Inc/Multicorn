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
    QueryRange, QuerySelect

from ...request import _AndOr



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
    orders = [None, QuerySelect, QueryFilter, QueryDistinct, QueryOrder, QueryRange]
    for index, sub_query in enumerate(self.queries):
        splitted_queries = [QueryChain(sublist) if sublist else None
                for sublist in (self.queries[:index], self.queries[index:])]
        if sub_query.__class__ in orders:
            if orders.index(sub_query.__class__) < orders.index(last_seen):
                access_point.site.logger.debug("In SQL : %s, In python: %s" %
                        (splitted_queries[0], splitted_queries[1]))
                return splitted_queries
            last_seen = sub_query.__class__
        else:
            return splitted_queries
        managed, not_managed = sub_query.alchemy_validate(
            access_point, properties)
        if not_managed is not None:
            access_point.site.logger.debug("In SQL : %s, In python: %s" %
                    (splitted_queries[0], splitted_queries[1]))
            return splitted_queries
        properties = sub_query.validate(properties)
    return self, None


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
        if isinstance(condition, _AndOr):
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
                           for new_name, values in cond_tree[name].items())
            else:
                return False
    if check_operators(self.condition) and \
        all(inner_manage(name, values, properties)
           for name, values in cond_tree.items()):
        return self, None
    else:
        return None, self



def query_select_validator(self, access_point, properties):
    """Validate that the query select can be managed from SQLAlchemy.

    A query select can be managed if the properties it aliases all belong to an
    Alchemy access point instance.

    """
    from . import Alchemy

    def isvalid(select, properties):
        """Check if ``select`` is valid according to ``properties``."""
        for value in select.mapping.values():
            if value.__class__ not in access_point.dialect.SUPPORTED_FUNCS:
                return False
        for name, sub_select in select.sub_selects.items():
            remote_ap = properties[name].remote_ap
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




QueryChain.alchemy_validate = query_chain_validator
QueryDistinct.alchemy_validate = standard_validator
QueryFilter.alchemy_validate = query_filter_validator
QueryOrder.alchemy_validate = standard_validator
QueryRange.alchemy_validate = standard_validator
QuerySelect.alchemy_validate = query_select_validator


# pylint: enable=W0613
