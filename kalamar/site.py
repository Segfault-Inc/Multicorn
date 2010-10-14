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
Site
====

Site class. Create one for each independent site with its own configuration.

"""

from .access_point import AccessPoint
from .request import normalize, make_request, And, Condition, Or, Not
from .query import QueryFilter, QuerySelect, QueryChain


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
        name = request.property.__repr__()
        if name in aliases:
            return Condition(aliases.get(name, name),
                             request.operator,
                             request.value)
        elif ".".join(name.split(".")[:-1] + ["*"]) in aliases:
            return request
        else:
            new_name = "____%s" % name.replace(".", "_")
            aliases[name] = new_name
            return Condition(new_name, request.operator, request.value)


def _delegate_to_acces_point(method_name, first_arg_is_a_request=False):
    """Create a function delegating ``method_name`` to an access point."""
    if first_arg_is_a_request:
        def wrapper(self, access_point, request=None, *args, **kwargs):
            """Call ``access_point.method_name(request, *args, **kwargs)``."""
            access_point = self.access_points[access_point]
            request = normalize(access_point.properties, request)
            return getattr(access_point, method_name)(
                request, *args, **kwargs)
    else:
        def wrapper(self, access_point, *args, **kwargs):
            """Call ``access_point.method_name(*args, **kwargs)``."""
            access_point = self.access_points[access_point]
            return getattr(access_point, method_name)(*args, **kwargs)
    wrapper.__name__ = method_name
    wrapper.__doc__ = getattr(AccessPoint, method_name).__doc__
    return wrapper


class Site(object):
    """Kalamar site."""
    def __init__(self):
        self.access_points = {}
    
    def register(self, name, access_point):
        if hasattr(access_point, "site"):
            raise RuntimeError("Access point already registered.")
        if name in self.access_points:
            raise RuntimeError(
                "Site already has an access point named %r." % name)
        access_point.site = self
        access_point.name = name
        self.access_points[name] = access_point

    def view(self, access_point, aliases=None, request=None, query=None):
        access_point = self.access_points[access_point]
        if aliases is None:
            aliases = {"": "*"}
        if query is None:
            # Add dummy selects to be able to filter on those
            aliases = dict(((value, key) for key, value in aliases.items()))
            request = make_request(request)
            request = _translate_request(request, aliases)
            aliases = dict(((value, key) for key, value in aliases.items()))
            query = QueryChain((QuerySelect(aliases), QueryFilter(request)))
        self.validate_query(access_point, query)
        return access_point.view(query)

    def validate_query(self, access_point, query):
        return query.validate(self, access_point.properties)

    create = _delegate_to_acces_point("create")
    delete = _delegate_to_acces_point("delete")
    delete_many = _delegate_to_acces_point("delete_many", True)
    open = _delegate_to_acces_point("open", True)
    search = _delegate_to_acces_point("search", True)
    save = _delegate_to_acces_point("save")
