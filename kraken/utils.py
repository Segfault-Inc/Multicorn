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
import sys
import hashlib
import datetime
import inspect
import mimetypes
import werkzeug
import urlparse
import posixpath
from werkzeug.contrib.securecookie import SecureCookie
from werkzeug.exceptions import NotFound, Forbidden

COOKIE_SECRET = None

def make_absolute_url(request, url):
    """
    # fake request for http://localhost/foo/
    >>> request = Request(werkzeug.create_environ(path='/foo/'))
    
    >>> make_absolute_url(request, 'http://localhost/foo/bar/')
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, '/foo/bar/')
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, './bar/')
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, 'bar/')
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, '../bar/')
    'http://localhost/bar/'

    # Same tests without the trailing slash
    >>> make_absolute_url(request, 'http://localhost/foo/bar')
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, '/foo/bar')
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, './bar')
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, 'bar')
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, '../bar')
    'http://localhost/bar'
    """
    parsed = urlparse.urlparse(url)
    if parsed.scheme and parsed.netloc:
        return url
    if not url.startswith('/'):
        path = request.base_url[len(request.host_url):]
        url = '/' + path + '/' + url
    new_url = request.host_url.rstrip('/') + posixpath.normpath(url)
    # posixpath.normpath always remove trailing slashes
    if url.endswith('/'):
        new_url += '/'
    return new_url

def redirect(request, url, status=302):
    """
    >>> @Request.application
    ... def test_app(request):
    ...     return redirect(request, request.args['redirect_to'],
    ...                     int(request.args.get('status', 302)))
    >>> client = werkzeug.Client(test_app)
    >>> client.get('/foo?redirect_to=../bar') # doctest: +ELLIPSIS
    (..., '302 FOUND', [...('Location', 'http://localhost/bar')...)
    """
    return werkzeug.redirect(make_absolute_url(request, url), status)

class Request(werkzeug.Request):
    @werkzeug.cached_property
    def session(self):
        return SecureCookie.load_cookie(self, secret_key=COOKIE_SECRET)

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
    """
    Return the nubmer of explicit arguments the function takes
    ie. without *args and **kwargs.
    
    >>> arg_count(lambda: 1)
    0
    >>> arg_count(lambda x, y: 1)
    2
    >>> arg_count(lambda x, y, *args: 1)
    2
    """
    args, varargs, varkw, defaults = inspect.getargspec(function)
    return len(args)

def runserver(wsgi_app, args=None):
    """
    Run a developpement server from command line for the given WSGI application.
    """
    if args is None:
        args = sys.argv[1:]
    action_runserver = script.make_runserver(wsgi_app)
    werkzeug.script.run(args=['runserver'] + args)
