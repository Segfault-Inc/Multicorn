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


def recursive_subclasses(class_):
    yield class_
    for sub in class_.__subclasses__():
        for sub_sub in recursive_subclasses(sub):
            yield sub_sub

class AccessPoint(object):
    """
    Abstact class for every storage backend
    """
    
    def __new__(cls, **kwargs):
        protocol = kwargs['url'].split(':')[0]
        for subclass in recursive_subclasses(cls):
            if getattr(subclass, 'protocol', None) == protocol:
                return subclass(**kwargs)
        raise ValueError('Unknown protocol: ' + protocol)
    
    def __init__(self, config):
        self.config = config
        self.default_encoding = config.get('default_encoding', 'utf-8')
        self.properties_aliases = dict(
            part.split('=', 1) for part in 
            config.get('properties_aliases', '').split('/')
        )
        
            
    def search(self, test_func):
        """
        List every item in entry_point that match test_func
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


