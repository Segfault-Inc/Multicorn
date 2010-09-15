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

from kalamar import Item


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
    
    def __init__(self):
        self.access_points = set()
    
    def register(self, access_point):
        if access_point.site:
            # TODO: raise specific exception?
            raise RuntimeError('Access point already registered.')
        access_point.site = self
        self.access_points.add(access_point)

    def view(self, access_point, mapping, request=None, **kwArgs):
        """Returns partial items.

        ``mapping`` is a dict mapping the items property to custom keys in 
        the returned partial items. 

        ``request`` follows the same format as in the search method.
        Example:
        site.view("access_point',{"name":"name","boss_name": "foreign.name"})

        """
        conditions = self.parse_request(request or [])
        master_ap = self.access_points[access_point]
        # The ap returns 
        return master_ap.view(mapping, conditions,**kwArgs)



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
    
    def open(self, access_point, request):
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
            print "MULTIPLE OBJECT!" + str(request)
            raise self.MultipleObjectsReturned

    def batchopen(self, access_point, requests):
        for request in requests:
            yield open(access_point, request)
    
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

    def generate_primary_values(self, access_point_name):
        """Return dict of primary keys and values for ``access_point_name``."""
        access_point = self.access_points[access_point_name]
        return access_point.generate_primary_values()
    
    def item_from_filename(self, filename):
        """Search all access points for an item matching ``filename``.

        Return the first item found or None.

        """
        filename = os.path.normpath(filename)
        for access_point in self.access_points.values():
            item = access_point.item_from_filename(filename)
            if item and item is not NotImplemented:
                return item

