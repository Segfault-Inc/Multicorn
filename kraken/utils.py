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

import os
import hashlib
import datetime
import inspect
import mimetypes
import werkzeug

class Request(werkzeug.Request):
    pass

class Response(werkzeug.Response):
    pass

class StaticFileResponse(object):
    """
    Respond with a static file, guessing the mimitype from the filename,
    and using WSGI’s ``file_wrapper`` when available.
    """
    
    def __init__(self, filename):
        self.filename = filename
    
    def __call__(self, environ, start_response):
        stat = os.stat(self.filename)
        etag = '%s,%s,%s' % (self.filename, stat.st_size, stat.st_mtime)
        etag = '"%s"' % hashlib.md5(etag).hexdigest()
        headers = [
            ('Date', werkzeug.http_date()),
            ('Etag', etag),
        ]
        mtime = datetime.datetime.utcfromtimestamp(stat.st_mtime)
        if not werkzeug.is_resource_modified(environ, etag=etag,
                                             last_modified=mtime):
            start_response('304 Not Modified', headers)
            return []
       
        mime_type, encoding = mimetypes.guess_type(self.filename)
        headers.extend((
            ('Content-Type', mime_type or 'application/octet-stream'),
            ('Content-Length', str(stat.st_size)),
            ('Last-Modified', werkzeug. http_date(stat.st_mtime))
        ))
        start_response('200 OK', headers)
        return werkzeug.wrap_file(environ, open(self.filename, 'rb'))

def arg_count(function):
    args, varargs, varkw, defaults = inspect.getargspec(function)
    if varargs or varkw:
        raise ValueError(u'Argument count is variable.')
    return len(args)

