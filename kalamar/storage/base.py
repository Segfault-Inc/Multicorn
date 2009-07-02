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
Default access point.

"AccessPoint" is the class to override in order to create storage access
points.

"""

from kalamar import utils
from kalamar.item import Item

class AccessPoint(object):
    """Abstact class for all storage backends."""
    
    @classmethod
    def from_url(cls, **config):
        """
        Return an instance of the appropriate class according to the URL.
        
        >>> AccessPoint.from_url(url='nonexistent-protocol://…')
        Traceback (most recent call last):
            ...
        ValueError: Unknown protocol: nonexistent-protocol

        """
        protocol = config['url'].split(':', 1)[0]
        for subclass in utils.recursive_subclasses(cls):
            if getattr(subclass, 'protocol', None) == protocol:
                return subclass(**config)
        raise ValueError('Unknown protocol: ' + protocol)
    
    def __init__(self, **config):
        """Common instance initialisation."""
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
        """Expand syntaxic sugar in requests.
        
        "conditions" is a list of (property_name, operator, value) tuples
        as returned by kalamar.site.Site.parse_request.
        
        If "operator" is None, set it to "kalamar.utils.equals".

        If "property_name" is None in the n-th condition, set it to 
        the n-th property of this access point.
        
        >>> ap = AccessPoint(url='', storage_aliases='a=p1/b=p2/c=p3')
        >>> list(ap.expand_syntaxic_sugar([
        ...     utils.Condition(None, None,              1),
        ...     utils.Condition(None, utils.operator.gt, 2),
        ...     utils.Condition('c', None,               3),
        ...     utils.Condition('d', utils.operator.ge,  4)
        ... ])) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        [Condition('a', <built-in function eq>, 1),
         Condition('b', <built-in function gt>, 2),
         Condition('c', <built-in function eq>, 3),
         Condition('d', <built-in function ge>, 4)]

        """
        for n, cond in enumerate(conditions):
            yield utils.Condition(cond.property_name or self.property_names[n],
                                  cond.operator or utils.operator.eq,
                                  cond.value)
    
    def search(self, conditions):
        """Generate a sequence of every item matching ``conditions''.
        
        "conditions" is a list of utils.Condition objects
        as returned by kalamar.site.Site.parse_request
        
        """
        # Algorithm:
        # 1. list the interesting storage properties
        # 2. expand syntaxic sugar
        # 3. divide conditions into two categories : parser and storage
        # 4. call _storage_search with storage conditions as parameters
        # 5. filter the items yielded with conditions applying to the parser
        # 6. yield filtered items
        
        storage_aliases = dict(self.storage_aliases)
        parser_aliases = dict(self.parser_aliases)
        
        storage_properties_not_aliased = set(self.get_storage_properties())
        # Parser aliased properties have priority over storage not aliased
        # properties
        storage_properties_not_aliased.difference_update(parser_aliases.keys())
        
        storage_properties = set(storage_aliases.keys())
        storage_properties.update(storage_properties_not_aliased)
        
        storage_conditions = []
        parser_conditions = []

        conditions = list(self.expand_syntaxic_sugar(conditions))
        for condition in conditions:
            if condition.property_name in storage_properties:
                storage_conditions.append(utils.Condition(
                    storage_aliases[condition.property_name],
                    condition.operator, condition.value))
            else:
                parser_conditions.append(condition)
        
        for properties, opener in self._storage_search(storage_conditions):
            item = Item.get_item_parser(self.config['parser'], self,
                                        opener, properties)
            
            for condition in parser_conditions:
                if not condition(item.properties):
                    break
            else:
                yield item
    
    def get_storage_properties(self):
        """Return the list of properties used by the storage (not aliased).

        This method has to be overriden.

        """
        raise NotImplementedError('Abstract method')
    
    def _storage_search(self, conditions):
        """Return a sequence of tuple (properties, file_opener).
        
        This method has to be overriden.

        Return values:
        - "properties" is a dictionnary
        - "file_opener" is a function that takes no argument and returns a
          file-like object
        
        """
        raise NotImplementedError('Abstract method')

    def save(self, item):
        """Update or add the item.

        This method has to be overriden.

        """
        raise NotImplementedError('Abstract method')

    def remove(self, item):
        """Remove/delete the item from the backend storage.

        This method has to be overriden.

        """
        raise NotImplementedError('Abstract method')

