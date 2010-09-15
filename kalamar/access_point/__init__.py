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
Access point base class.

"""

class AccessPoint(object):
    """Abstract class for all storage backends.
    
    :param config: dictionnary of the access point configuration
    :param default_encoding: default character encoding used if the parser does
        not have one (read-only)
    :param property_names: properties defined in the access_point configuration
    :param url: where the data is available
    :param basedir: directory from where relatives pathes should start

    """
    def __init__(self, **config):
        """Common instance initialisation."""
        self.config = config
        self.name = config.get("name")
        self.site = config["site"]
        self.default_encoding = config.get("default_encoding", "utf-8")
        self.properties = config["properties"]
        self.url = config["url"]
        self.basedir = config.get("basedir", "")
