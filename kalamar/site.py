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
Site
====

Site class. Create one for each independent site with its own configuration.

"""

from .request import normalize, make_request, And, Condition, Or, Not
from .query import QueryFilter, QuerySelect, QueryChain, QueryOrder, QueryRange


def _translate_request(request, aliases):
    """Translate high-level ``request`` to low-level using ``aliases``."""
    if isinstance(request, And):
        return And(*(_translate_request(req, aliases)
                     for req in request.sub_requests))
    elif isinstance(request, Or):
        return Or(*(_translate_request(req, aliases)
                    for req in request.sub_requests))
    elif isinstance(request, Not):
        return Not(_translate_request(request.sub_request, aliases))
    elif isinstance(request, Condition):
        name = repr(request.property)
        if name in aliases:
            return Condition(
                aliases.get(name, name), request.operator, request.value)
        elif name in aliases.values():
            return Condition(name, request.operator, request.value)
        elif ".".join(name.split(".")[:-1] + ["*"]) in aliases:
            return request
        else:
            new_name = "__%s" % name.replace(".", "_")
            aliases[name] = new_name
            return Condition(new_name, request.operator, request.value)


def _delegate_to_acces_point(method_name, first_arg_is_a_request=False):
    """Create a function delegating ``method_name`` to an access point."""
    if first_arg_is_a_request:
        def wrapper(self, access_point_name, request=None, *args, **kwargs):
            """Call ``access_point.method_name(request, *args, **kwargs)``."""
            access_point = self.access_points[access_point_name]
            request = normalize(access_point.properties, request)
            return getattr(access_point, method_name)(
                request, *args, **kwargs)
    else:
        def wrapper(self, access_point_name, *args, **kwargs):
            """Call ``access_point.method_name(*args, **kwargs)``."""
            access_point = self.access_points[access_point_name]
            return getattr(access_point, method_name)(*args, **kwargs)
    # Redefining documentation and name of the wrappers
    # pylint: disable=W0622
    wrapper.__name__ = method_name
    wrapper.__doc__ = \
        "Call :meth:`kalamar.access_point.AccessPoint.%s`." % method_name
    # pylint: enable=W0622
    return wrapper


class Site(object):
    """Kalamar site."""
    def __init__(self):
        self.access_points = {}
    
    def register(self, name, access_point):
        """Add an access point to this site.

        :param name: Identifier string of the added access point.
        :param access_point: Concrete subclass of :class:`AccessPoint`.

        """
        if name in self.access_points:
            raise RuntimeError(
                "Site already has an access point named %r." % name)
        self.access_points[name] = access_point
        access_point.bind(self, name)

    def view(self, access_point_name, aliases=None, request=None, order_by=None,
            select_range=None, query=None):
        """Call :meth:`kalamar.access_point.AccessPoint.view`.

        If ``alias`` and ``request`` are given, a query is created from them.

        The query is then validated and then passed to the ``view`` method of
        the acess point called ``access_point_name``.

        """
        access_point = self.access_points[access_point_name]
        if aliases is None:
            aliases = {"": "*"}
        if query is None:
            # Add dummy selects to be able to filter on those
            chain = []
            aliases = dict(((value, key) for key, value in aliases.items()))
            request = make_request(request)
            request = _translate_request(request, aliases)
            aliases = dict(((value, key) for key, value in aliases.items()))
            chain.append(QuerySelect(aliases))
            chain.append(QueryFilter(request))
            if order_by is not None:
                chain.append(QueryOrder(order_by))
            if select_range is not None:
                if hasattr(select_range, "__iter__"):
                    select_range = slice(*select_range)
                else: 
                    select_range = slice(select_range)
                chain.append(QueryRange(select_range))
            query = QueryChain(chain)
        query.validate(self, access_point.properties)
        return access_point.view(query)

    create = _delegate_to_acces_point("create")
    delete = _delegate_to_acces_point("delete")
    delete_many = _delegate_to_acces_point("delete_many", True)
    open = _delegate_to_acces_point("open", True)
    search = _delegate_to_acces_point("search", True)
    save = _delegate_to_acces_point("save")
