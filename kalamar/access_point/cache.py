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


def _invalidate_cache(function):
    """Override ``function`` which needs to invalidate the cache when called."""
    def wrapper(cache_access_point, *args, **kwargs):
        """Wrap ``access_point`` by invalidating the cache."""
        # ``wrapper`` is in a decorator and can access private attributes
        # pylint: disable=W0212
        cache_access_point._cache.clear()
        return function(cache_access_point, *args, **kwargs)
        # pylint: enable=W0212
    return wrapper


class Cache(AccessPointWrapper):
    """Access point that store a cache in memory for search request.

    The cache is invalided when data changes (with :meth:`save`,
    :meth:`delete`, :meth:`delete_many`).

    """
    def __init__(self, wrapped_ap):
        super(Cache, self).__init__(wrapped_ap)
        self._cache = {}

    def search(self, request):
        # Try to hit the cache
        values = self._cache.get(request, None)
        if not values:
            values = [ItemWrapper(self, item)
                for item in self.wrapped_ap.search(request)]
            self._cache[request] = values
        return values

    @_invalidate_cache
    def delete(self, item):
        super(Cache, self).delete(item)

    @_invalidate_cache
    def delete_many(self, request):
        super(Cache, self).delete_many(request)

    @_invalidate_cache
    def save(self, item):
        super(Cache, self).save(item)
