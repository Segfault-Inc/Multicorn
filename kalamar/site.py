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
Kalamar Site class.

Create one for each independent site with its own configuration.

"""

import os
import warnings

from kalamar.storage import base
from kalamar.config import baseparser
from kalamar import Item, requestparser, utils



class Site(object):
    """Kalamar site."""
    class NotOneObjectReturned(Exception):
        """Not one object has been returned."""

    class MultipleObjectsReturned(NotOneObjectReturned):
        """More than one object have been returned."""

    class ObjectDoesNotExist(NotOneObjectReturned):
        """No object has been returned."""

    class FileNotFoundError(Exception):
        """File not found on filesystem."""
    
    def __init__(self, configs, fail_on_inexistent_parser=True):
        """Create a kalamar site from a configuration file.
        

        """
        self.access_points = {}
        for config in configs:
            config.site = self
            if fail_on_inexistent_parser:
                access_point = base.AccessPoint.from_url(config)
            else:
                try:
                    access_point = base.AccessPoint.from_url(config)
                except utils.ParserNotAvailable, exception:
                    warnings.warn('The access point %r was ignored. (%s)' %
                                  (config.name, exception.args[0]))
                    continue
            self.access_points[config.name] = access_point
                        
        # If no configuration file, no access_point !
    
    @staticmethod
    def parse_request(request):
        """Convert a ``request`` to a list of Condition objects.

        If ``request`` is a string, parse it with our query language.
        If it’s a number, parse its string representation.
        If it’s a dict, assume equality for all operators.
        Otherwise, assume it’s a list of values
        
        >>> Site.parse_request(u"/'1'/b='42'/c>='3'/")
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        [Condition(None, None, u'1'),
         Condition(u'b', <built-in function eq>, u'42'),
         Condition(u'c', <built-in function ge>, u'3')]

        >>> Site.parse_request({u'a': 1, u'b': None})
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        [Condition(u'a', None, 1),
         Condition(u'b', None, None)]

        >>> Site.parse_request(1)            # doctest: +NORMALIZE_WHITESPACE
        [Condition(None, None, 1)]

        """
        if isinstance(request, dict):
            return [utils.Condition(key, None, value)
                    for key, value in request.items()]
        elif isinstance(request, int) or isinstance(request, float):
            return requestparser.parse(str(request))
        elif isinstance(request, basestring):
            return requestparser.parse(request)
        else:
            return [value if isinstance(value, utils.Condition)
                    else utils.Condition(None, None, value)
                    for value in request]
        
    def isearch(self, access_point, request=None):
        """Return a generator of items in ``access_point`` matching ``request``.
        
        See ``Site.parse_request`` for the syntax of the ``request`` string.

        """
        conditions = self.parse_request(request or [])
        return self.access_points[access_point].search(conditions)
    
    def search(self, access_point, request=None):
        """List all items in ``access_point`` matching ``request``.
        
        See ``Site.parse_request`` for the syntax of the ``request`` string.

        """
        return list(self.isearch(access_point, request))
    
    def open(self, access_point, request=None):
        """Return the item in access_point matching request.
        
        If there is no result, raise ``Site.ObjectDoesNotExist``.
        If there are more than 1 result, raise ``Site.MultipleObjectsReturned``.
        
        """
        search = iter(self.isearch(access_point, request))
        try:
            item = search.next()
        except StopIteration:
            raise self.ObjectDoesNotExist
        
        try:
            search.next()
        except StopIteration:
            return item
        else:
            raise self.MultipleObjectsReturned
    
    @staticmethod
    def save(item):
        """Update or add the item."""
        return item._access_point.save(item)

    @staticmethod
    def remove(item):
        """Remove/delete the item from the backend storage."""
        return item._access_point.remove(item)
    
    def remove_many(self, access_point, request):
        """Remove all items matching the request
        """
        conditions = self.parse_request(request or [])
        return self.access_points[access_point].remove_many(conditions)

    
    def create_item(self, access_point_name, properties):
        """Return an item.
        
        TODO document & test

        """
        access_point = self.access_points[access_point_name]
        return Item.create_item(access_point, properties)
    
    def get_description(self, access_point_name):
        """Return a tuple of strings or None.
        
        Return the keys defined in configuration or None if
        ``access_point_name`` does not exist.

        """
        access_point = self.access_points[access_point_name]
        return access_point.property_names
    
    def item_from_filename(self, filename):
        """Search all access points for an item matching ``filename``.

        Return the first item found or None.

        """
        filename = os.path.normpath(filename)
        for access_point in self.access_points.values():
            item = access_point.item_from_filename(filename)
            if item and item is not NotImplemented:
                return item


class SiteFromConf(Site):

    def __init__(self,filename=None,fail_on_inexistent_parser=True):
        super(SiteFromConf,self).__init__(baseparser.parse(filename),fail_on_inexistent_parser)


