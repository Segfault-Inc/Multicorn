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
    
    def __init__(self, access_point, accessor_properties, opener):
        self._opener = opener
        self._stream = None
        self.properties = ItemProperties(self)
        #TODO : this maynot have to be like that (priority of the properties
        #from the accessor over the extractor ... or the contrary ?)
        self.properties.update(accessor_properties)
        self.access_point = access_point
    
    def matches(self, prop_name, operator, value):
        """Return boolean
        
        Check if the item's property <prop_name> matches <value> for the given
        operator.
        
        Availables operators are :
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
            return re.match(value, prop_val)
          
        elif operator == "~!=" or operator == "!~=":
            return re.match(value, prop_val)
          
        else
            raise OperatorNotAvailable(operator)
    
    class OperatorNotAvailable(Exception): pass
    
    def serialize(self):
        """Return the item serialized into a string"""
        raise NotImplementedError("Abstract class")
    
    def _convert_value_type(self, prop_name, value):
        """Do nothing by default"""
        return value
    
    def _get_encoding(self):
        """Return a string
        
        Return the item's encoding, based on what the extractor can know from
        the items's data or, if unable to do so, on what is specified in the
        access_point.
        
        """
        raise NotImplementedError("Abstract class")
    
    def _read_property(self, prop_name):
        """Read a property form the item data and return a string
        
        If the property does not exist, returns None.
        ***This method have to be overloaded***
        
        """
        raise NotImplementedError("Abstract class")
     
     def _open(self):
        """Open the stream when called for the first time."""
        if not(self._stream):
            self._stream = self._opener()

class AtomItem(Item):
    """An indivisible block of data"""
    
    def read(self):
        """Alias for properties["_content"]"""
        return self.properties["_content"]
    
    def write(self, value):
        """Alias for properties["_content"] = value"""
        self.properties["_content"] = value

class CapsuleItem(Item, list):
    """An ordered list of Items (atoms or capsules)
    
    A capsule is a multiparts item
    
    """
    pass

class ItemProperties(dict):
    """A class that acts like a defaultdict used as a properties storage.
    
    You have to give a reference to the item to the constructor.
    
    """
    
    def __init__(self, item):
        self._item = item
    
    def __getitem__(self, key):
        try:
            res = super.__getitem__(key)
        except KeyError:
            res = _item._read_property(key)
        return res

