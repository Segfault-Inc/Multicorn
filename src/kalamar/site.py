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

"""
TODO : Change this docstring
Create one for each
independent site with it’s own configuration.
"""

class Site(object):
    """Create a kalamar site from a configuration file."""
    
    class NotOneObjectReturned(Exception): pass
    class MultipleObjectsReturned(NotOneObjectReturned): pass
    class ObjectDoesNotExist(NotOneObjectReturned): pass
    
    def __init__(self, config_filename=None):
        pass
    
    def search(self, access_point, request):
        """List every item in access_point that match request"""
        raise NotImplementedError # TODO
    
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
        raise NotImplementedError # TODO

    def remove(self, item):
        """
        Remove/delete the item from the backend storage
        """
        raise NotImplementedError # TODO

class Item:
    """An abstract class used by Capsule and Atom.
    
    It represents every item you can get with kalamar.
    
    """
    
    def __init__(self):
      self.properties = {} # TODO : This should be an instance of a class
    
    def matches(prop_name, operator, value):
        """matches(prop_name, operator, value) -> boolean
        
        Check if the item's property <prop_name> matches <value> for the given
        operator.
        
        Standards availables operators are :
        - "=" -> equal
        - "!=" -> different
        - ">" -> greater than (alphabetically)
        - "<" -> lower than (alphabetically)
        - ">=" -> greater or equal (alphabetically)
        - "<=" -> lower or equal (alphabetically)
        - "~=" -> matches the given regexp
        - "~!=" -> does not match the given regexp (same as "!~=")
        TODO : explain what regexp are availables
        
        Some descendants of Item class may want to overload the appropriates
        fonctions to get the "greater than/lower than" operators working with a
        numerical order (for instance).
        
        """
        
        prop_val = self.properties[prop_name]
        value = _convert_value_type(prop_name, value)
        
        if operator == "=":
            return prop_val == value
          
        elif operator == "!=":
            return prop_val != value
          
        elif operator == ">":
            return prop_val > value
          
        elif operator == "<":
            return prop_val < value
          
        elif operator == ">=":
            return prop_val >= value
          
        elif operator == "<=":
            return prop_val <= value
          
        elif operator == "~=":
            raise NotImplementedError # TODO
          
        elif operator == "~!=" or operator == "!~=":
            raise NotImplementedError # TODO
          
        else
            raise OperatorNotAvailable(operator)
    
    class OperatorNotAvailable(Exception): pass
    
    def _convert_value_type(prop_name, value):
        """Do nothing by default"""
        return value
        
    access_point = property(_get_access_point, _set_access_point)
    extractor = property(_get_extractor)
    accessor = property(_get_accessor)
    
    def _get_properties(self):
        raise NotImplementedError # TODO
    
    def _set_properties(self):
        raise NotImplementedError # TODO
    
    def _get_access_point(self):
        raise NotImplementedError # TODO
    
    def _get_extractor(self):
        raise NotImplementedError # TODO
    
    def _get_accessor(self):
        raise NotImplementedError # TODO

