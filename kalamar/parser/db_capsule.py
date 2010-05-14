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
Parsers for databases.

"""

from kalamar.storage import base
from kalamar.item import CapsuleItem
from kalamar.utils import Condition, operators



class OneToManyDBCapsule(CapsuleItem):
    """TODO doc
    
    """
    format = 'onetomany_db_capsule'

    @property
    def _old_subitems(self):
        """List of subitems present when the capsule was created or saved."""
        if not hasattr(self, '__old_subitems'):
            # First call to ``_old_subitems``, initialize ``subitems``
            return self._load_subitems()
        return self.__old_subitems
    
    def _load_subitems(self):
        """Load and return capsule subitems."""
        # Search items in link table matching self keys
        request = [
            Condition(foreign_key, operators['='], self[capsule_key])
            for foreign_key, capsule_key in zip(
                self._access_point.config['children_foreign_keys'].split('/'),
                self._access_point.config['capsule_keys'].split('/'))]
                
        items = self._access_point.site.search(
            self._access_point.config['children_access_point'], request)
        sorting_column = self._access_point.config['children_sorting_column']
        items.sort(key=lambda item: item[sorting_column])
        self.__old_subitems = items
        return items

    def serialize(self):
        """Save all subitems in the linking table."""
        sorting_column = self._access_point.config['children_sorting_column']
        keys = zip(
            self._access_point.config['children_foreign_keys'].split('/'),
            self._access_point.config['capsule_keys'].split('/'))
        
        for subitem in self._old_subitems:
            # Delete old item from physical storage before doing anything else
            # to avoid data collision when saving data
            self._access_point.site.remove(subitem)
            if subitem not in self.subitems:
                # Remove all subitems not in ``self.subitems``
                for foreign_key, capsule_key in keys:
                    subitem[foreign_key] = None
                    # TODO: setting an unused rank is useful here not to erase
                    #       older data using the same primary key
                    self._access_point.site.save(subitem)

        for subitem in self.subitems:
            # Update order and foreign keys in kalamar item
            subitem[sorting_column] = self.subitems.index(subitem)
            for foreign_key, capsule_key in keys:
                subitem[foreign_key] = self[capsule_key]
            # Save item
            self._access_point.site.save(subitem)

class ManyToManyDBCapsule(CapsuleItem):
    """A capsule format for items stored in databases.
    
    This parser can store capsules in databases without additional public
    access points.
    
    A table is needed in the same database as the capsule (but not necessarily
    in the same database as the subitems). This table is called the linking
    table, as it links the capsule access point and the item access point.
    
    """
    format = 'manytomany_db_capsule'

    def __init__(self, access_point, opener=None, storage_properties={}):
        """DBCapsule instance initialisation."""
        super(ManyToManyDBCapsule, self).__init__(
            access_point, opener, storage_properties)
        self._link_ap = None
    
    def _load_subitems(self):
        """Load and return capsule subitems.
        
        This function also creates a private access point for the linking
        table.

        """
        capsule_url = self._access_point.config['url']
        self.capsule_table_name = capsule_url.split('?')[-1]
        self.foreign_access_point_name = \
            self._access_point.config['foreign_access_point']
        link_table_name = self._access_point.config['link_table']
        self.capsule_keys = self._access_point.config['capsule_keys'].split('/')
        self.foreign_keys = self._access_point.config['foreign_keys'].split('/')
        self.link_capsule_keys = \
            self._access_point.config['link_capsule_keys'].split('/')
        self.link_foreign_keys = \
            self._access_point.config['link_foreign_keys'].split('/')
        self.link_sort_key = self._access_point.config['order_by']
        link_access_point_name = '_%s_%s' % (
            self.capsule_table_name, self.foreign_access_point_name)

        # Create an access point if not already done
        if not self._link_ap:
            config = {
                'basedir': self._access_point.basedir,
                'name': link_access_point_name,
                'url': '%s?%s' % (
                    capsule_url.split('?')[0],
                    link_table_name)}
            self._link_ap = base.AccessPoint.from_url(**config)
            self._access_point.site.access_points[link_access_point_name] = \
                self._link_ap

        # Search items in link table matching self keys
        request = [
            Condition(link_capsule_key, operators['='], self[capsule_key])
            for capsule_key, link_capsule_key
            in zip(self.capsule_keys, self.link_capsule_keys)]
        items = self._access_point.site.search(link_access_point_name, request)
        items.sort(key=lambda x: x[self.link_sort_key])
        
        # Return items in foreign table matching link item keys
        for item in items:
            request = [
                Condition(foreign_key, operators['='], item[link_foreign_key])
                for foreign_key, link_foreign_key
                in zip(self.foreign_keys, self.link_foreign_keys)]
            item = self._access_point.site.open(
                self.foreign_access_point_name, request)
            yield item

    def serialize(self):
        """Save all subitems in the linking table."""
        self.subitems # trigger _load_subitems
        
        # Remove all items in link table matching self keys
        # TODO: this can be optimised
        request = [
            Condition(link_capsule_key, operators['='], self[capsule_key])
            for capsule_key, link_capsule_key
            in zip(self.capsule_keys, self.link_capsule_keys)]
        self._access_point.site.remove_many(
            self._link_ap.config['name'], request)
        
        for number, subitem in enumerate(self.subitems):
            properties = {}
            
            for capsule_key, link_capsule_key in zip(
                self.capsule_keys,self.link_capsule_keys):
                properties[link_capsule_key] = self[capsule_key]
            
            for foreign_key, link_foreign_key in zip(
                self.foreign_keys, self.link_foreign_keys):
                properties[link_foreign_key] = subitem[foreign_key]
            
            item = self._access_point.site.create_item(
                self._link_ap.config['name'], properties)
            item[self.link_sort_key] = number
            self._access_point.site.save(item)



class GenericManyToManyDBCapsule(CapsuleItem):
    """A capsule format for items stored in databases.
    
    This parser can store capsules in databases without additional public
    access points.
    
    A table is needed in the same database as the capsule (but not necessarily
    in the same database as the subitems). This table is called the linking
    table, as it links the capsule access point and the item access point.
    
    """
    format = 'generic_manytomany_db_capsule'

    def __init__(self, access_point, opener=None, storage_properties={}):
        """DBCapsule instance initialisation."""
        super(GenericManyToManyDBCapsule, self).__init__(
            access_point, opener, storage_properties)
        self._link_ap = None
    
    def _load_subitems(self):
        """Load and return capsule subitems.
        
        This function also creates a private access point for the linking
        table.

        """
        capsule_url = self._access_point.config['url']
        self.capsule_table_name = capsule_url.split('?')[-1]
        link_table_name = self._access_point.config['link_table']
        self.capsule_keys = self._access_point.config['capsule_keys'].split('/')
        self.link_capsule_keys = \
            self._access_point.config['link_capsule_keys'].split('/')
        self.link_sort_key = self._access_point.config['order_by']
        self.access_point_key = self._access_point.config['access_point_key']
        self.request_key = self._access_point.config['request_key']
        
        link_access_point_name = '_%s_%s' % (
            self.capsule_table_name, link_table_name)

        # Create an access point if not already done
        if not self._link_ap:
            config = {
                'basedir': self._access_point.basedir,
                'name': link_access_point_name,
                'url': '%s?%s' % (capsule_url.split('?')[0], link_table_name)}
            self._link_ap = base.AccessPoint.from_url(**config)
            self._access_point.site.access_points[link_access_point_name] = \
                self._link_ap

        # Search items in link table matching self keys
        request = [
            Condition(link_capsule_key, operators['='], self[capsule_key])
            for capsule_key, link_capsule_key
            in zip(self.capsule_keys, self.link_capsule_keys)]
        items = self._access_point.site.search(link_access_point_name, request)
        items.sort(key=lambda x: x[self.link_sort_key])
        
        # Return items in foreign access points matching link item keys
        for link_item in items:
            request = link_item[self.request_key]
            access_point_name = link_item[self.access_point_key]
            
            item = self._access_point.site.open(
                access_point_name, request)            
            yield item

    def serialize(self):
        """Save all subitems in the linking table."""
        self.subitems # trigger _load_subitems
        
        # Remove all items in link table matching self keys
        # TODO: this can be optimised
        request = [
            Condition(link_capsule_key, operators['='], self[capsule_key])
            for capsule_key, link_capsule_key
            in zip(self.capsule_keys, self.link_capsule_keys)]
        self._access_point.site.remove_many(
            self._link_ap.config['name'], request)

        # Save all link items
        for number, subitem in enumerate(self.subitems):
            properties = {}
            
            for capsule_key, link_capsule_key \
            in zip(self.capsule_keys,self.link_capsule_keys):
                properties[link_capsule_key] = self[capsule_key]
            
            properties[self.access_point_key] = subitem.access_point_name
            properties[self.request_key] = subitem.request
            
            link_item = self._access_point.site.create_item(
                self._link_ap.config['name'], properties)
            link_item[self.link_sort_key] = number
            self._access_point.site.save(link_item)
