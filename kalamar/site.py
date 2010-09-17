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

from .item import Item
from .request import Request, ViewRequest


class Site(object):
    """Kalamar site."""
    
    def __init__(self):
        self.access_points = {}
    
    def register(self, name, access_point):
        if hasattr(access_point, 'site'):
            # TODO: raise specific exception?
            raise RuntimeError('Access point already registered.')
        if name in self.access_points:
            # TODO: raise specific exception?
            raise RuntimeError('Site already has an access point named %r.'
                               % name)
        access_point.site = self
        access_point.name = name
        self.access_points[name] = access_point
    
    def deleguate_to_acces_point(method_name, first_arg_is_a_request=False):
        if first_arg_is_a_request:
            def wrapper(self, access_point, request=None, *args, **kwargs):
                request = Request.parse(request)
                ap = self.access_points[access_point]
                return getattr(ap, method_name)(request, *args, **kwargs)
        else:
            def wrapper(self, access_point, *args, **kwargs):
                ap = self.access_points[access_point]
                return getattr(ap, method_name)(*args, **kwargs)
        wrapper.__name__ = method_name
        return wrapper
   

    open = deleguate_to_acces_point('open', True)
    search = deleguate_to_acces_point('search', True)
    delete_many = deleguate_to_acces_point('delete_many', True)
    save = deleguate_to_acces_point('save')
    delete = deleguate_to_acces_point('delete')
    create = deleguate_to_acces_point('create')
    def view(self, access_point, aliases, request):
        ap = self.access_points[access_point]
        request = Request.parse(request)
        return ap.view(ViewRequest(aliases,request))

    del deleguate_to_acces_point
