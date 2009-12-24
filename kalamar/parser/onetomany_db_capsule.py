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
    # TODO: make this capsule ordered
    format = 'onetomany_db_capsule'

    
    def _load_subitems(self):
        """Load and return capsule subitems.
        """
        # Search items in link table matching self keys
        request = [
            Condition(foreign_key, operators['='], self[capsule_key])
            for foreign_key, capsule_key in zip(
                self._access_point.config['children_foreign_keys'].split('/'),
                self._access_point.config['capsule_keys'].split('/'),
            )
        ]
                
        items = self._access_point.site.search(
            self._access_point.config['children_access_point'],
            request
        )
        sorting_column = self._access_point.config['children_storting_column']
        items.sort(key=lambda item: item[sorting_column])
        self._old_subitems = items
        return items

    def serialize(self):
        """Save all subitems in the linking table."""
        sorting_column = self._access_point.config['children_storting_column']
        keys = zip(
            self._access_point.config['children_foreign_keys'].split('/'),
            self._access_point.config['capsule_keys'].split('/'),
        )
        
        self.subitems # trigger _load_subitems

        # TODO: do this only for items that are not in self.subitems anymore
        # To do that we probably need the "primary keys" of the subitems,
        # but that only makes sense in a database
        for subitem in self._old_subitems:
            for foreign_key, capsule_key in keys:
                subitem[foreign_key] = None
            self._access_point.site.save(subitem)
                

        for number, subitem in enumerate(self.subitems):
            number += 1 # enumerate counts from 0, we want numbers from 1
            
            subitem[sorting_column] = number
            for foreign_key, capsule_key in keys:
                subitem[foreign_key] = self[capsule_key]
            self._access_point.site.save(subitem)

