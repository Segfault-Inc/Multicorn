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


def make_cache(ap_cls):
    '''
    Return an *ap_cls* AccessPoint enhanced with cache behavior.

    Ie, each search request can avoid the ap_cls search when search was
    previously done. 

    Please note that make_cache returns a class inherited from *ap_cls*.
    '''

    class Cache(ap_cls):
        """
        Access point that store a cache in memory for search request.

        The cache is invalided when data changes (with save, delete, delete_many)

        """
        def __init__(self, *args, **kwargs):
            super(Cache, self).__init__(*args, **kwargs)

            self.__cache = {}

        def search(self, request):
            # try to hit the cache
            ret = self.__cache.get(request, None)
            if not ret:
                ret = list(super(Cache, self).search(request))
                self.__cache[request] = ret

            return ret

        # override functions which needs to invalidate the cache when called
        def invalid_cache(f):
            def _(self, *args, **kwargs):
                self.__cache = {}
                return f(self, *args, **kwargs)
            return _

        save = invalid_cache(ap_cls.save)
        delete_many = invalid_cache(ap_cls.delete_many)
        delete = invalid_cache(ap_cls.delete)

    return Cache
