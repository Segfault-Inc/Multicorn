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
TODO : Change this docstring
Create one for each
independent site with its own configuration.
"""

import os
import kalamar
import ConfigParser
from kalamar import utils
from kalamar.storage import AccessPoint

class Site(object):
    """Create a kalamar site from a configuration file."""
    
    class NotOneObjectReturned(Exception): pass
    class MultipleObjectsReturned(NotOneObjectReturned): pass
    class ObjectDoesNotExist(NotOneObjectReturned): pass
    
    class FileNotFoundError(Exception): pass
    
    def __init__(self, config_filename=None):
        c = ConfigParser.RawConfigParser()
        config_filename = os.path.realpath(config_filename)
        if c.read(config_filename) == []:
            raise self.FileNotFoundError(config_filename)
        basedir = os.path.dirname(config_filename)
        self.access_points = {}
        for section in c.sections():
            kwargs = dict(c.items(section), basedir=basedir)
            self.access_points[section] = AccessPoint.from_url(**kwargs)
    
    @staticmethod
    def parse_request(request):
        """Convert a ``request`` string to (prop_name, operator, value) tuples
        
        >>> list(Site.parse_request(u'/1/b=42/c>=3/')) # doctest: +ELLIPSIS
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        [(None, None,                   u'1'),
         (u'b', <built-in function eq>, u'42'),
         (u'c', <built-in function ge>, u'3')]
        """
        request = unicode(request)
        for part in request.split(u'/'):
            if not part:
                continue
            for operator_str, operator_func in utils.operators.items():
                try:
                    pos = part.index(operator_str)
                except ValueError:
                    continue
                else:
                    yield (
                        part[:pos], # property name
                        operator_func,
                        part[pos + len(operator_str):], # value
                    )
                    break
            else:
                # no operator found
                yield (None, None, part)
        
                
    def search(self, access_point, request):
        """List every item in ``access_point`` that match ``request``
        
        See ``Site.parse_request`` for the syntax of the ``request`` string.
        """
        conditions = self.parse_request(request)
        return self.access_points[access_point].search(conditions)
    
    def open(self, access_point, request):
        """Return the item in access_point that match request
        
        If there is no result, raise Site.ObjectDoesNotExist
        If there is more than one result, raise Site.MultipleObjectsReturned
        
        """
        it = iter(self.search(access_point, request))
        try:
            obj = it.next()
        except StopIteration:
            raise self.ObjectDoesNotExist
        
        try:
            it.next()
        except StopIteration:
            return obj
        else:
            raise self.MultipleObjectsReturned
    
    def save(self, item):
        """Update or add the item"""
        return item._access_point.save(item)

    def remove(self, item):
        """
        Remove/delete the item from the backend storage
        """
        return item._access_point.remove(item)

