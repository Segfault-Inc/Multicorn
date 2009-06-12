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
# along with Kalamar library.  If not, see <http://www.gnu.org/licenses/>.

"""This module contains base classes to make kalamar items.

You probably want to inherit from one of the followings :
 - CapsuleItem
 - AtomItem

"""

from accesspoint import AccessPoint

class BaseItem:
    """An abstract class used by Capsule and Atom.
    
    It represents every item you can get with kalamar.
    Data :
     - properties : acts like a defaultdict. The keys are strings and the values
       are python objects (for curious ones : this is a lazy implementation).
     - access_point : where in kalamar, is stored the item. It is an instance
       of AccessPoint.
    
    """
    
    def __init__(self, access_point):
        self.properties = {} # TODO : This should be an instance of a class
        self.access_point = access_point
    
    def matches(self, prop_name, operator, value):
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
        
        Some descendants of Item class may want to overload _convert_value_type
        to get the "greater than/lower than" operators working with a numerical
        order (for instance).
        
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
    
    def _convert_value_type(self, prop_name, value):
        """Do nothing by default"""
        return value
    
    def _get_encoding(self):
        """_get_encoding() -> return a string
        
        Return the item's encoding, based on what the extractor can know from
        the items's data or, if unable to do so, on what is specified in the
        access_point.
        
        """
        raise NotImplementedError # TODO

class AtomItem(Item):
    """An indivisible block of data"""
    
    def read(self):
        """Alias for properties["_content"]"""
        return self.properties["_content"]
    
    def write(self, value):
        """Alias for properties["_content"] = value"""
        self.properties["_content"] = value

class CapsuleItem(Item):
    """An ordered list of Items (atoms or capsules)
    
    A capsule is a multiparts item
    
    """
    
    def list(self):
        """list() -> return list
        
        Returns a list of kalamar requests giving access to a single item.
        
        """
        raise NotImplementedError # TODO
    
    def add_item(self, an_item):
        """Add an item to the capsule"""
        raise NotImplementedError # TODO

