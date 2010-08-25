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
        ``isearch``, ``search``, ``open``, ``item_from_filename``
    All cached entries are removed when the following methods are called:
        ``save``, ``remove``

    Warning: arguments for cached methods must be hashable.
    
    >>> class FakeKalamar(object):
    ...     y = 1
    ...     def search(self, x, y=None):
    ...         print 'search', x
    ...         if isinstance(x, int):
    ...             return [x + self.y]
    ...         elif x == '*':
    ...             return [1, 2, 3]
    ...     def save(self, y):
    ...         print 'save', y
    ...         self.y = y
    ...     def remove(self, y):
    ...         print 'remove', y
    ...     def item_from_filename(self, z):
    ...         print 'filename', z
    ...         return int(z) + self.y
    ...     def test(self):
    ...         print 'test', self.y
    ...     ObjectDoesNotExist = StandardError('Object does not exist')
    ...     MultipleObjectsReturned = StandardError('Multiple objects')
    >>> kalamar = CachedKalamarSite(FakeKalamar())
    >>> kalamar.search(2)
    search 2
    [3]
    >>> kalamar.search(5)
    search 5
    [6]
    >>> kalamar.search(2) # result is cached: FakeKalamar.search is not called
    [3]
    >>> for i in kalamar.isearch(2): print i # result is cached too with isearch
    3
    >>> kalamar.save(-1)
    save -1
    >>> kalamar.search(2) # cache has been invalidated
    search 2
    [1]
    >>> kalamar.search(2) # result is cached
    [1]
    >>> kalamar.remove(2)
    remove 2
    >>> kalamar.search(2) # cache has been invalidated
    search 2
    [1]
    >>> kalamar.item_from_filename('6')
    filename 6
    5
    >>> kalamar.item_from_filename('6') # result is cached
    5
    >>> kalamar.search('2') # '2' is no int, return nothing
    search 2
    >>> kalamar.open('2') # '2' is no int, raise Error
    Traceback (most recent call last):
    ...
    StandardError: Object does not exist
    >>> kalamar.open('*') # '*' means [1,2,3], raise Error
    Traceback (most recent call last):
    ...
    StandardError: Multiple objects
    >>> kalamar.test()
    test -1

    """
    def __init__(self, kalamar_site):
        """TODO docstring"""
        self.kalamar_site = kalamar_site
        self._cache = {}
    
    def isearch(self, access_point, request=None):
        return iter(self.search(access_point, request))
    
    def search(self, access_point, request=None):
        # We use here the given request as part of a key to find item in cache
        # if this search has already been done. If the request is not hashable,
        # create a hashable object from request.
        if isinstance(request, dict):
            # TODO: test this!
            # lstrip('u') removes the leading 'u' in front of unicode values
            # rstrip('L') removes the ending 'L' behind long integers
            string_request = '/'.join(
                ['%s=%s' % (key, repr(value).lstrip('u').rstrip('L'))
                 for key, value in request.items()])
        elif isinstance(request, list):
            string_request = tuple(request)

        key = ('search', access_point, string_request)
        if key in self._cache:
            return self._cache[key]
        else:
            value = self.kalamar_site.search(access_point, request)
            self._cache[key] = value
            return value
    
    def item_from_filename(self, filename):
        key = ('item_from_filename', filename)
        try:
            return self._cache[key]
        except KeyError:
            value = self.kalamar_site.item_from_filename(filename)
            self._cache[key] = value
            return value
    
    def open(self, access_point, request=None):
        """Return the item in access_point matching request.
        
        If there is no result, raise ``Site.ObjectDoesNotExist``.
        If there are more than 1 result, raise ``Site.MultipleObjectsReturned``.
        
        """
        results = self.search(access_point, request)
        if not results:
            raise self.kalamar_site.ObjectDoesNotExist
        if len(results) > 1:
            raise self.kalamar_site.MultipleObjectsReturned
        return results[0]

    def save(self, *args, **kwargs):
        """TODO docstring"""
        # This changes the data. Flush the whole cache
        self._cache = {}
        return self.kalamar_site.save(*args, **kwargs)
        
    def remove(self, *args, **kwargs):
        """TODO docstring"""
        # This changes the data. Flush the whole cache
        self._cache = {}
        return self.kalamar_site.remove(*args, **kwargs)

    def __getattr__(self, name):
        """Proxy for other methods."""
        return getattr(self.kalamar_site, name)
