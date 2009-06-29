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
from kalamar.item import Item
from pprint import pprint

class AccessPoint(object):
    """
    Abstact class for every storage backend
    """
    
    @classmethod
    def from_url(cls, **config):
        """
        Return the an instance of the correct class according to the URL
        """
        
        protocol = config['url'].split(':', 1)[0]
        for subclass in utils.recursive_subclasses(cls):
            if getattr(subclass, 'protocol', None) == protocol:
                return subclass(**config)
        raise ValueError('Unknown protocol: ' + protocol)
    
    def __init__(self, **config):
        self.config = config
        self.default_encoding = config.get('default_encoding', 'utf-8')
        for prop in 'storage_aliases', 'parser_aliases':
            setattr(self, prop, [
                tuple(part.split('=', 1))
                for part in config.get(prop, '').split('/') if '=' in part
            ])
        self.property_names = [name for name, alias
                               in self.storage_aliases + self.parser_aliases]
        self.url = config['url']
        self.basedir = config.get('basedir', '')
            
    def expand_syntaxic_sugar(self, conditions):
        """Expand syntaxic sugar in requests
        
        ``conditions`` is a list of (property_name, operator, value) tuples
        as returned by kalamar.site.Site.parse_request
        
        If ``operator`` is None, set it to ``kalamar.utils.equals``.
        If ``property_name`` is None in the n-th condition, set it to 
        the n-th property of this access point.
        
        >>> ap = AccessPoint(url='', storage_aliases='a=p1/b=p2/c=p3')
        >>> list(ap.expand_syntaxic_sugar([
        ...     (None, None,              1), 
        ...     (None, utils.operator.gt, 2), 
        ...     ('c', None,               3), 
        ...     ('d', utils.operator.ge,  4)
        ... ])) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        [('a', <built-in function eq>, 1),
         ('b', <built-in function gt>, 2),
         ('c', <built-in function eq>, 3),
         ('d', <built-in function ge>, 4)]
        """
        for n, (property_name, operator, value) in enumerate(conditions):
            if operator is None:
                operator = utils.operator.eq
            if property_name is None:
                property_name = self.property_names[n]          
            yield property_name, operator, value
    
    def search(self, conditions):
        """
        Generate a sequence of every item matching ``conditions''.
        
        ``conditions`` is a list of (property_name, operator, value) tuples
        as returned by kalamar.site.Site.parse_request
        
        """
        # Algorithm:
        # 1. expand syntaxic sugar.
        # 2. divide conditions into two categories : parser and storage
        # 3. call _storage_search with storage conditions as parameters.
        # 4. filter the items yielded with conditions applying to the parser.
        # 5. yield filtered items
        
        conditions = list(self.expand_syntaxic_sugar(conditions))

        storage_conditions = []
        parser_conditions = []
        parser_aliases_values = [a for (a,b) in self.parser_aliases]
        
        sto_props_old = self.get_storage_properties()
        sto_aliases = dict(self.storage_aliases)
        sto_aliases_rev = dict((b,a) for (a,b) in self.storage_aliases)
        sto_props = set(sto_aliases_rev.get(prop, prop)
                        for prop in sto_props_old)
        sto_props.update(sto_props_old)
        for name, funct, value in conditions:
            if name in sto_props:
                storage_conditions.append((sto_aliases[name], funct, value))
            else:
                parser_conditions.append((name, funct, value))
        
        for properties, opener in self._storage_search(storage_conditions):
            item = Item.get_item_parser(self.config['parser'], self,
                                        opener, properties)
            
            for name, funct, value in parser_conditions:
                if not funct(item.properties[name], value):
                    break
            else:
                yield item
    
    def get_storage_properties(self):
        """Return the list of properties used by the storage (not aliased)"""
        raise NotImplementedError # subclasses need to override this
    
    def _storage_search(self, conditions):
        """Return a sequence of tuple (properties, file_opener).
        
        ``properties`` is a dictionnary.
        ``file_opener`` is a function that takes no argument and returns a
        file-like object.
        
        """
        # subclasses need to override this
        raise NotImplementedError('Abstract method')

    def save(self, item):
        """
        Update or add the item
        """
        # subclasses need to override this
        raise NotImplementedError('Abstract method')

    def remove(self, item):
        """
        Remove/delete the item from the backend storage
        """
        # subclasses need to override this
        raise NotImplementedError('Abstract method')


