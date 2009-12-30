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

class ManyToManyDBCapsule(CapsuleItem):
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
        self.foreign_access_point_name =\
            self._access_point.config['foreign_access_point']
        link_table_name = self._access_point.config['link_table']
        self.capsule_keys = self._access_point.config['capsule_keys'].split('/')
        self.foreign_keys = self._access_point.config['foreign_keys'].split('/')
        self.link_capsule_keys =\
            self._access_point.config['link_capsule_keys'].split('/')
        self.link_foreign_keys =\
            self._access_point.config['link_foreign_keys'].split('/')
        self.link_sort_key = self._access_point.config['order_by']
        link_access_point_name = '_%s_%s' % (
            self.capsule_table_name,
            self.foreign_access_point_name
        )

        # Create an access point if not already done
        if not self._link_ap:
            config = {
                'name': link_access_point_name,
                'url': '%s?%s' % (
                    capsule_url.split('?')[0],
                    link_table_name
                )
            }
            self._link_ap = base.AccessPoint.from_url(**config)

            keys = self._link_ap.get_storage_properties()
            self._access_point.site.access_points[link_access_point_name] =\
                self._link_ap

        # Search items in link table matching self keys
        request = [
            Condition(link_capsule_key, operators['='], self[capsule_key])
            for capsule_key, link_capsule_key
            in zip(self.capsule_keys, self.link_capsule_keys)
        ]
        items = self._access_point.site.search(link_access_point_name, request)
        items.sort(key=lambda x: x[self.link_sort_key])
        
        # Used in self.serialize to know wich subitems have been removed from 
        # the capsule
        #self._old_link_items = items[:]
        

        # Return items in foreign table matching link item keys
        for item in items:
            request = [
                Condition(foreign_key, operators['='], item[link_foreign_key])
                for foreign_key, link_foreign_key
                in zip(self.foreign_keys, self.link_foreign_keys)
            ]
            item = self._access_point.site.open(
                self.foreign_access_point_name,
                request
            )
            yield item

    def serialize(self):
        """Save all subitems in the linking table."""
        self.subitems # trigger _load_subitems
        
        # Remove all items in link table matching self keys
        # TODO: this can be optimised
        request = [
            Condition(link_capsule_key, operators['='], self[capsule_key])
            for capsule_key, link_capsule_key
            in zip(self.capsule_keys, self.link_capsule_keys)
        ]
        self._access_point.site.remove_many(self._link_ap.config['name'],
                                            request)
        
        for number, subitem in enumerate(self.subitems):
            properties = {}
            
            for capsule_key, link_capsule_key in zip(
                self.capsule_keys,self.link_capsule_keys
            ):
                properties[link_capsule_key] = self[capsule_key]
            
            for foreign_key, link_foreign_key in zip(
                self.foreign_keys, self.link_foreign_keys
            ):
                properties[link_foreign_key] = subitem[foreign_key]
            
            item = self._access_point.site.create_item(
                self._link_ap.config['name'], properties
            )
            item[self.link_sort_key] = number
            self._access_point.site.save(item)
