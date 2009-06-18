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
    
    @classmethod
    def from_url(cls, **config):
        """
        Return the an instance of the correct class according to the URL
        """
        if cls is not AccessPoint:
            return super(AccessPoint, cls).__new__(cls)
        
        protocol = config['url'].split(':', 1)[0]
        for subclass in utils.recursive_subclasses(cls):
            if getattr(subclass, 'protocol', None) == protocol:
                return subclass(**config)
        raise ValueError('Unknown protocol: ' + protocol)
    
    def __init__(self, **config):
        self.config = config
        self.default_encoding = config.get('default_encoding', 'utf-8')
        for prop in 'accessor_aliases', 'parser_aliases':
            setattr(self, prop, [
                tuple(part.split('=', 1))
                for part in config.get(prop, '').split('/') if '=' in part
            ])
        self.property_names = [name for name, alias in self.accessor_aliases]
        self.url = config['url']
        self.basedir = config.get('basedir', '')
            
    def expand_syntaxic_sugar(self, conditions):
        """Expand syntaxic sugar in requests
        
        ``conditions`` is a list of (property_name, operator, value) tuples
        as returned by kalamar.site.Site.parse_request
        
        If ``operator`` is None, set it to ``kalamar.utils.equals``.
        If ``property_name`` is None in the n-th condition, set it to 
        the n-th property of this access point.
        
        >>> ap = AccessPoint(url='', accessor_aliases='a=p1/b=p2/c=p3')
        >>> list(ap.expand_syntaxic_sugar([
        ...     (None, None,                  1), 
        ...     (None, utils.greater_than,    2), 
        ...     ('c', None,                   3), 
        ...     ('d', utils.greater_or_equal, 4)
        ... ])) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        [('a', <function equals at 0x...>,           1), 
         ('b', <function greater_than at 0x...>,     2), 
         ('c', <function equals at 0x...>,           3), 
         ('d', <function greater_or_equal at 0x...>, 4)]
        """
        for n, (property_name, operator, value) in enumerate(conditions):
            if operator is None:
                operator = utils.equals
            if property_name is None:
                property_name = self.property_names[n]          
            yield property_name, operator, value
    
    def search(self, conditions):
        """
        List every item in that match ``conditions``
        
        ``conditions`` is a list of (property_name, operator, value) tuples
        as returned by kalamar.site.Site.parse_request
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


