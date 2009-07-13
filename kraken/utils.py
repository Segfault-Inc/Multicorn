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
# along with Kraken.  If not, see <http://www.gnu.org/licenses/>.

"""
Various utilities for Kraken
"""

import mimetypes
import werkzeug

class Request(werkzeug.Request):
    pass

class Response(werkzeug.Response):
    pass

class StaticFileResponse(Response):
    """
    Respond with a static file, guessing the mimitype from the filename,
    and using WSGI’s ``file_wrapper`` when available.
    """
    
    def __init__(self, filename):
        self.filename = filename
        mimetype, encoding = mimetypes.guess_type(filename)
        super(StaticFileResponse, self).__init__(mimetype=mimetype)
    
    def __call__(self, environ, start_response):
        # Wrap here so that __init__ doesn’t need a reference to environ
        self.response = werkzeug.wrap_file(environ, open(self.filename, 'rb'))
        self.direct_passthrough = True
        return super(StaticFileResponse, self).__call__(environ, start_response)

