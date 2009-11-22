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

class DBCapsule(CapsuleItem):
    """TODO docstring
    
    """
    # TODO: make this capsule ordered
    format = 'db_capsule'
    
    def _load_subitems(self):
        capsule_url = self._access_point.config['url']
        capsule_name = self._access_point.config['name']
        foreign = self._access_point.config['foreign']
        config = {
            # TODO: 'capsulename_foreign' should be in config
            'name': '%s_%s' % (capsule_name, foreign),
            # TODO: 'capsuleurl_foreign' should be in config
            'url': '%s_%s' % (capsule_url, foreign)}
        ap = base.AccessPoint.from_url(config)

        keys = ap.get_storage_properties()
        capsule_keys = [key for key in keys if key.beginswith(capsule_name)]
        foreign_keys = [key for key in keys if key.beginswith(foreign)]
        request = '/'.join([('%s=%s' % (key, self[key.split('_', 1)[1]]))
                            for key in capsule_keys])
        return self._access_point.site.search(ap, request)
