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
DataBase.

This parser can store capsules in databases without additional access points.

TODO: documentation.

"""

from kalamar.storage import base
from kalamar.item import CapsuleItem

class DBCapsule(CapsuleItem):
    """TODO docstring
    
    """
    # TODO: make this capsule ordered
    format = 'db_capsule'
    
    def _load_subitems(self):
        capsule_url = self._access_point.config['url']
        capsule_name = capsule_url.split('?')[-1]
        foreign = self._access_point.config['foreign']
        ap_name = '_%s_%s' % (capsule_name, foreign)
        config = {
            # TODO: 'capsulename_foreign' should be in config
            'name': ap_name,
            # TODO: 'capsuleurl_foreign' should be in config
            'url': '%s_%s' % (capsule_url, foreign)}
        ap = base.AccessPoint.from_url(**config)
        self._access_point.site.access_points[ap_name] = ap

        keys = ap.get_storage_properties()
        capsule_keys = [key for key in keys if key.startswith(capsule_name)]
        foreign_keys = [key for key in keys if key.startswith(foreign)]
        request = '/'.join(
            ['%s=%s' % (key, self[key.split('_', 1)[1]])
             for key in capsule_keys])
        items = self._access_point.site.search(ap_name, request)

        foreign_requests = ['/'.join(
                ['%s=%s' % (key.split('_', 1)[1], item[key])
                 for key in foreign_keys]) for item in items]

        return [self._access_point.site.open(foreign, request)
                for request in foreign_requests]
                
    def serialize(self):
        capsule_url = self._access_point.config['url']
        capsule_name = capsule_url.split('?')[-1]
        foreign = self._access_point.config['foreign']
        ap_name = '_%s_%s' % (capsule_name, foreign)

        config = {
            # TODO: 'capsulename_foreign' should be in config
            'name': ap_name,
            # TODO: 'capsuleurl_foreign' should be in config
            'url': '%s_%s' % (capsule_url, foreign)}
        ap = base.AccessPoint.from_url(**config)
        keys = ap.get_storage_properties()
        capsule_keys = [key for key in keys if key.startswith(capsule_name)]
        foreign_keys = [key for key in keys if key.startswith(foreign)]

        for subitem in self.subitems:
            properties = {}
            for key in capsule_keys:
                properties[key] = self[key.split('_', 1)[1]]
            for key in foreign_keys:
                properties[key] = subitem[key.split('_', 1)[1]]
            item = self._access_point.site.create_item(ap_name, properties)
            self._access_point.site.save(item)
