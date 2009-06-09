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
Instances of the Site class are WSGI applications.  Create one for each
independent site with it’s own configuration.
"""

__all__ = ['Site']

from pprint import pformat
from werkzeug import Request, Response


class Site(object):
    """
    Create a WSGI application from a configuration file.
    """
    
    def __init__(self, config_filename=None):
        pass
    
    @Request.application
    def __call__(self, request):
        """WSGI entry point for every HTTP request"""
        
        return Response('Hello, World!')

