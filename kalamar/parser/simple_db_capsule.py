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

class OneToMany_DB_capsule(CapsuleItem):
    """TODO doc
    
    """
    # TODO: make this capsule ordered
    format = 'onetomany_db_capsule'

    
    def _load_subitems(self):
        """Load and return capsule subitems.
        
        This function also creates a private access point for the linking
        table.

        """
        # TODO: keys in linking table should be configurable
        foreign_access_point_name = \
            self._access_point.config['foreign_access_point']
        sorting_column = self._access_point.config['children_storting_column']

        capsule_keys = self._access_point._get_primary_keys()

        # Search items in link table matching self keys
        request = '/'.join([
            '%s=%s' % (key, self[key])
            for key in capsule_keys
        ])
                
        items = self._access_point.site.search(foreign_access_point_name,
                                               request)
        items.sort(key=lambda item: item[sorting_column])
        return items

    def serialize(self):
        """Save all subitems in the linking table."""
        sorting_column = self._access_point.config['children_storting_column']
        capsule_keys = self._access_point._get_primary_keys()

        for number, subitem in enumerate(self.subitems):
            number += 1 # enumerate counts from 0, we want numbers from 1
            
            subitem[sorting_column] = number
            for key in capsule_keys:
                subitem[key] = self[key]
            self._access_point.site.save(subitem)

