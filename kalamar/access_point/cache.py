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
Cache
=====

Access point caching properties.

"""

from . import AccessPointWrapper
from ..item import ItemWrapper


class Cache(AccessPointWrapper):
    """Access point that store a cache in memory for search request.

    The cache is invalided when data changes (with :meth:`save`,
    :meth:`delete`, :meth:`delete_many`).

    """
    def __init__(self, wrapped_ap):
        super(Cache, self).__init__(wrapped_ap)
        self.__cache = {}

    def search(self, request):
        # Try to hit the cache
        values = self.__cache.get(request, None)
        if not values:
            values = [ItemWrapper(self, item)
                for item in self.wrapped_ap.search(request)]
            self.__cache[request] = values

        return values

    # Override functions which needs to invalidate the cache when called
    def invalidate_cache(name):
        def wrapper(self, *args, **kwargs):
            self.__cache.clear()
            return getattr(super(Cache, self), name)(*args, **kwargs)
        wrapper.__name__ = name
        return wrapper

    save = invalidate_cache("save")
    delete_many = invalidate_cache("delete_many")
    delete = invalidate_cache("delete")
