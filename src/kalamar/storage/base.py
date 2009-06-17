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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.


from kalamar import utils

class AccessPoint(object):
    """
    Abstact class for every storage backend
    """
    
    def __new__(cls, **kwargs):
        if cls is not AccessPoint:
            return super(AccessPoint, cls).__new__(cls)
        
        protocol = kwargs['url'].split(':')[0]
        for subclass in utils.recursive_subclasses(cls):
            if getattr(subclass, 'protocol', None) == protocol:
                return subclass(**kwargs)
        raise ValueError('Unknown protocol: ' + protocol)
    
    def __init__(self, **config):
        self.config = config
        self.default_encoding = config.get('default_encoding', 'utf-8')
        for prop in 'accessor_aliases', 'parser_aliases':
            setattr(self, prop, dict(
                part.split('=', 1)
                for part in config.get(prop, '').split('/') if part
            ))
        self.url = config['url']
        self.basedir = config['basedir']
            
    def search(self, conditions):
        """
        List every item in that match ``conditions``
        
        ``conditions`` is a list as returned by kalamar.site.Site.parse_request
        """
        raise NotImplementedError # subclasses need to override this

    def save(self, item):
        """
        Update or add the item
        """
        raise NotImplementedError # subclasses need to override this

    def remove(self, item):
        """
        Remove/delete the item from the backend storage
        """
        raise NotImplementedError # subclasses need to override this


