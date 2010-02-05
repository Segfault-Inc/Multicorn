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
# along with Kraken.  If not, see <http://www.gnu.org/licenses/>.

"""
Kalamar module to use cache with sites.

"""


class CachedKalamarSite(object):
    """Kalamar cache wrapper.

    Wrapper for kalamar that caches results of the following methods:
        ``isearch``, ``search``, ``open``, ``item_from_filenames``
    All cached entries are removed when the following methods are called:
        ``save``, ``remove``

    Warning: arguments for cached methods must be hashable.
    
    >>> class FakeKalamar(object):
    ...     y = 1
    ...     def search(self, x):
    ...         print 'search', x
    ...         return x + self.y
    ...     def save(self, y):
    ...         print 'save', y
    ...         self.y = y
    >>> kalamar = CachedKalamarSite(FakeKalamar())
    >>> kalamar.search(2)
    search 2
    3
    >>> kalamar.search(5)
    search 5
    6
    >>> kalamar.search(2) # result is cached: FakeKalamar.search is not called
    3
    >>> kalamar.save(-1)
    save -1
    >>> kalamar.search(2) # cache has been invalidated
    search 2
    1

    """
    def __init__(self, kalamar_site):
        """TODO docstring"""
        self.kalamar_site = kalamar_site
        self._cache = {}
    
    def _cached_method(method_name):
        """TODO docstring"""
        def _wrapper(self, *args, **kwargs):
            """TODO docstring"""
            key = (method_name, args, tuple(sorted(kwargs.items())))
            try:
                # TODO all args and kwargs must be hashable
                # (use tuples instead of lists)
                return self._cache[key]
            except KeyError:
                value = getattr(self.kalamar_site, method_name)(*args, **kwargs)
                self._cache[key] = value
            return value
        _wrapper.__name__ = method_name
        return _wrapper
    
    isearch = _cached_method('isearch')
    search = _cached_method('search')
    open = _cached_method('open')
    item_from_filename = _cached_method('item_from_filename')
    del _cached_method
    
    def save(self, *args, **kwargs):
        """TODO docstring"""
        self._cache = {}
        return self.kalamar_site.save(*args, **kwargs)
        
    def remove(self, *args, **kwargs):
        """TODO docstring"""
        self._cache = {}
        return self.kalamar_site.remove(*args, **kwargs)

    def __getattr__(self, name):
        """Proxy for other methods."""
        return getattr(self.kalamar_site, name)
