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

``AccessPoint`` is the class to override in order to create storage access
points.

"""

from kalamar import utils
from kalamar.item import Item
from kalamar import parser
from kalamar import storage
from itertools import product



class AccessPoint(object):
    """Abstract class for all storage backends.
    
    Attributes:

    - config: a kalamar.Config instance
    - default_encoding: default character encoding used if the parser does
      not have one. Read-only attribute.
    - property_names: properties defined in the access_point configuration.
    - url: where the data is available.
    - basedir: directory from where relatives pathes should start.

    """
    @classmethod
    def from_url(cls, config):
        """Return an instance of the appropriate class according to the Config instance
        >>> from kalamar.config import Config 
        >>> AccessPoint.from_url(Config('nonexistent-protocol://…',"nonexistent",{}))
        Traceback (most recent call last):
            ...
        ValueError: Unknown protocol: nonexistent-protocol

        """
        protocol = config.url.split(':', 1)[0]
        storage.load()
        for subclass in utils.recursive_subclasses(cls):
            if getattr(subclass, 'protocol', None) == protocol:
                return subclass(config)
        raise ValueError('Unknown protocol: ' + protocol)
    
    def __init__(self, config):
        """Common instance initialisation."""
        self.config = config
        self.name = config.name
        self.site = config.site
        self.default_encoding = config.default_encoding
        self.property_names = config.properties.keys()
        self.properties = config.properties
        self.url = config.url
        self.basedir = config.basedir
        self.content_attr = None

    def expand_syntaxic_sugar(self, conditions):
        """Expand syntactic sugar in requests.
        
        ``conditions`` is a list of (property_name, operator, value) tuples
        as returned by kalamar.site.Site.parse_request.
        
        If ``operator`` is None, set it to ``kalamar.utils.equals``.

        If ``property_name`` is None in the n-th condition, set it to 
        the n-th property of this access point.
        
        Fixture
        >>> from kalamar.config import Config 
        >>> conf = Config('','',{"storage_aliases":[("a","p1"),("b","p2"),("c","p3")]})
        >>> ap = AccessPoint(conf)
        
        Test
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
        for i, cond in enumerate(conditions):
            yield utils.Condition(cond.property_name or self.property_names[i],
                                  cond.operator or utils.operator.eq,
                                  cond.value)


    def get_associated_storage(property_name):
        """ Return the name of the storage to which the property belongs.

        """
        if property_name in self.property_names :
            return self.name
        else : 
            return None


    
    def _relative_prop(self,prop):
        return str.join(".", prop.split(".")[1:])

    def _process_mapping(self, mapping):
        not_managed_mapping = {}
        managed_mapping = {}
        for key,value in mapping.items():
            splitted = value.split(".")
            if len(splitted) == 1:
                managed_mapping[key] = value
            else:
                prop = splitted[0]
                if prop in self.remote_properties:
                    remote_ap = self.remote_properties[prop]
                    if remote_ap in not_managed_mapping:
                        not_managed_mapping[remote_ap].update({key:value})
                    else:
                        not_managed_mapping[remote_ap]={key:value}
                else:
                    managed_mapping[key] = value
        return managed_mapping,not_managed_mapping
            
    def _process_mapping_conditions(self, conditions):
        not_managed_conditions = {}
        managed_conditions = []
        for cond in conditions:
            if cond.property_name:
                splitted = cond.property_name.split(".")
                if len(splitted) == 1:
                    managed_conditions.append(cond)
                else:
                    prop = splitted[0]
                    if prop in self.remote_properties:
                        remote_ap = self.remote_properties[prop]
                        newcond = utils.Condition(self._relative_prop(cond.property_name),cond.operator,cond.value)
                        condlist = not_managed_conditions.get(remote_ap,[])
                        condlist.append(newcond)
                        not_managed_conditions[remote_ap] = condlist
                    else:
                         managed_conditions.append(cond)
            else:
                    managed_conditions.append(cond)
        return managed_conditions,not_managed_conditions
        

    def _gen_sub_request(self, item, not_managed_mapping):
        conds = []
        for key,value in not_managed_mapping.items():
            base_object = value.split(".")[0]
            conds.append(utils.Condition(None,None,item.properties[base_object]))
        return conds


    def _fetch_join(self, item, remote_ap, remote_mapping,conditions=[]):
        new_mapping = dict([(prop_name,self._relative_prop(prop_mapping)) for prop_name,prop_mapping in
                            remote_mapping.items()])
        conditions = self._gen_sub_request(item,remote_mapping) + conditions
        for viewitem in self.site.view(remote_ap,new_mapping,conditions):
            yield viewitem



    def view(self, mapping, conditions,joins={}):
        """ This default implementation uses search. It must be overriden.
        
        """
        managed_mapping, not_managed_mapping = self._process_mapping(mapping)
        managed_conditions, not_managed_conditions = self._process_mapping_conditions(conditions)

        for item in self.site.search(self.name, managed_conditions):
            viewitem = dict([(alias,item[prop]) for alias, prop in managed_mapping.items()])
            if len(not_managed_mapping) != 0:
                subitems_generators = [self._fetch_join(item,remote_ap,remote_mapping,not_managed_conditions.get(remote_ap,[])) for remote_ap,remote_mapping in not_managed_mapping.items()]
                for cartesian_item in product(*subitems_generators):
                    for item in cartesian_item:
                        viewitem.update(item)
                    yield viewitem
            else:
                yield viewitem


    
    def search(self, conditions):
        """Generate a sequence of every item matching "conditions".
        
        "conditions" is a list of utils.Condition objects as returned by
        kalamar.site.Site.parse_request.
        
        """
        # Algorithm:
        # 1. list the interesting storage properties
        # 2. expand syntaxic sugar
        # 3. divide conditions into two categories : parser and storage
        # 4. call _storage_search with storage conditions as parameters
        # 5. filter the items yielded with conditions applying to the parser
        # 6. yield filtered items
        oldconds = conditions 
        conditions = list(self.expand_syntaxic_sugar(conditions))
        for properties, opener, lazy_properties in self._storage_search(conditions):
            item = self._make_item(opener, properties, lazy_properties)
            yield item
    
    def _make_item(self, opener, properties,lazy_properties={}):
        return Item(self, opener, properties,lazy_properties)
    
    def get_properties(self):
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

    def remove_many(self, conditions):
        """Remove all items matching the request."""
        for item in self.search(conditions):
            self.remove(item)

    def item_from_filename(self, filename):
        """Search for an item matching this filename.
        
        Storage that do not store items in files should leave this
        implementation that return the NotImplemented constant.

        Else, this method has to be overriden.

        """
        return NotImplemented

    @property
    def primary_keys(self):
        """List of primary keys names.
        
        Here, "primary key" must be understood as "a sufficient set of keys to
        make a request returning 0 or 1 object".

        This list must be ordered and stable for a given access point, in order
        to construct canonical requests for items.
        
        This property has to be overriden.

        """
        raise NotImplementedError('Abstract method')

    @property
    def remote_properties(self):
        """Dict of properties managed by remote access_point.

        The dict provides the properties names as keys, and the 
        corresponding access_points names as values."""
        raise NotImplementedError('Abstract method')
        

    def generate_primary_values(self):
        """Generate a dict with primary keys and unused values.

        This function is particularly useful to create new items on a storage
        unable to automatically generate meaningless primary keys (such as
        filesystems, or databases without sequences).

        This property has to be overriden.

        """
        raise NotImplementedError('Abstract method')
