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

from .request import normalize, make_request, And, Condition, Or, Not
from .query import QueryFilter, QuerySelect, QueryChain


def validate_filter(site, query, properties):
    valid = normalize(properties, query.condition)
    return valid, properties

def validate_select(site, query, properties):
    def derive_property(property, old_prop):
        if property.child_property is not None:
            childname = property.child_property.property_name
            try:
                child_prop = site.access_points[old_prop.remote_ap].properties[childname]
            except KeyError:
                raise KeyError("This request has no %r property" % childname) 
            return derive_property(property.child_property, child_prop)
        else:
            return old_prop
    new_props = {}
    for name, prop in query.mapping.items():
        old_prop = properties[prop.property_name]
        new_props[name] = derive_property(prop, old_prop)
    return True, new_props 


def validate_chain(site, query,  properties):
    for q in query.queries:
        valid, properties = QUERY_VALIDATORS[q.__class__](site, q, properties)
        if not valid:
            return False, properties
    return True, properties


QUERY_VALIDATORS = {
        QueryFilter: validate_filter,
        QuerySelect: validate_select,
        QueryChain:  validate_chain,
}


def translate_request(request, aliases):
    if isinstance(request, And):
        return And(*(translate_request(r, aliases)
                     for r in request.sub_requests))
    elif isinstance(request, Or):
        return Or(*(translate_request(r, aliases)
                    for r in request.sub_requests))
    elif isinstance(request, Not):
        return Not(translate_request(request.sub_request, aliases))
    elif isinstance(request, Condition):
        name = request.property.__repr__()
        if name in aliases:
            return Condition(aliases.get(name, name),
                         request.operator,
                         request.value)
        else:
            new_name = "____%s" % name.replace(".", "_")
            aliases[name] = new_name
            return Condition(new_name, request.operator, request.value)



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
        if query is None:
            #Add dummy selects to be able to filter on those
            aliases = dict([(value, key) for key, value in aliases.items()])
            request = make_request(request)
            request = translate_request(request, aliases)
            aliases = dict([(value, key) for key, value in aliases.items()])
            query = QueryChain([QuerySelect(aliases), QueryFilter(request)])
        valid, properties = self.validate_query(access_point, query)
        if valid is True:
            return access_point.view(query)
        else:
            raise RuntimeError("Bad Query")

 
    def validate_query(self, access_point, query):
        return QUERY_VALIDATORS[query.__class__](self, query, access_point.properties)       

    def deleguate_to_acces_point(method_name, first_arg_is_a_request=False):
        if first_arg_is_a_request:
            def wrapper(self, access_point, request=None, *args, **kwargs):
                access_point = self.access_points[access_point]
                request = normalize(access_point.properties, request)
                return getattr(access_point, method_name)(
                    request, *args, **kwargs)
        else:
            def wrapper(self, access_point, *args, **kwargs):
                access_point = self.access_points[access_point]
                return getattr(access_point, method_name)(*args, **kwargs)
        wrapper.__name__ = method_name
        return wrapper

    open = deleguate_to_acces_point("open", True)
    search = deleguate_to_acces_point("search", True)
    delete_many = deleguate_to_acces_point("delete_many", True)
    save = deleguate_to_acces_point("save")
    delete = deleguate_to_acces_point("delete")
    create = deleguate_to_acces_point("create")

    del deleguate_to_acces_point
