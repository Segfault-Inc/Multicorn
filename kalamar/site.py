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
import ConfigParser

from kalamar import storage, utils

class Site(object):
    """Kalamar site."""
    
    class NotOneObjectReturned(Exception): pass
    class MultipleObjectsReturned(NotOneObjectReturned): pass
    class ObjectDoesNotExist(NotOneObjectReturned): pass
    class FileNotFoundError(Exception): pass
    
    def __init__(self, config_filename=None):
        """Create a kalamar site from a configuration file."""
        config = ConfigParser.RawConfigParser()
        config_filename = os.path.realpath(config_filename)

        if not config.read(config_filename):
            raise self.FileNotFoundError(config_filename)

        basedir = os.path.dirname(config_filename)
        self.access_points = {}
        for section in config.sections():
            kwargs = dict(config.items(section), basedir=basedir)
            self.access_points[section] = storage.AccessPoint.from_url(**kwargs)
    
    @staticmethod
    def parse_request(request):
        """Convert a "request" string to (prop_name, operator, value) tuples.
        
        >>> list(Site.parse_request(u'/1/b=42/c>=3/')) # doctest: +ELLIPSIS
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        [Condition(None, None, u'1'),
         Condition(u'b', <built-in function eq>, u'42'),
         Condition(u'c', <built-in function ge>, u'3')]

        """
        request = unicode(request)
        for part in request.split(u'/'):
            if not part:
                continue
            for operator_str, operator_func in utils.operators.items():
                if operator_str in part:
                    name, value = part.split(operator_str, 1)
                    yield utils.Condition(name, operator_func, value)
                    break
            else:
                # No operator found
                yield utils.Condition(None, None, part)
        
    def search(self, access_point, request):
        """List all items in "access_point" matching "request".
        
        See "Site.parse_request" for the syntax of the "request" string.

        """
        conditions = self.parse_request(request)
        return self.access_points[access_point].search(conditions)
    
    def open(self, access_point, request):
        """Return the item in access_point matching request.
        
        If there is no result, raise "Site.ObjectDoesNotExist".
        If there is more than one result, raise "Site.MultipleObjectsReturned".
        
        """
        search = iter(self.search(access_point, request))
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
    
    def save(self, item):
        """Update or add the item."""
        return item._access_point.save(item)

    def remove(self, item):
        """Remove/delete the item from the backend storage."""
        return item._access_point.remove(item)
