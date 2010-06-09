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
Various utilities for Kraken.

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
# Forbidden is not used here but is made available for sites
from werkzeug.exceptions import NotFound, Forbidden

import kalamar.site



def make_absolute_url(request, url):
    """Return a clean absolute URL from ``request`` and ``url``.

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
    >>> make_absolute_url(request, '/')
    'http://localhost/'

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
    if urlparse.urlparse(url).netloc:
        # The URL has a 'host' part: it’s already absolute
        return url
    if not url.startswith('/'):
        # Relative to the current URL, not the site root
        path = request.base_url[len(request.host_url):]
        url = '/' + path + '/' + url
    new_url = request.host_url.rstrip('/') + posixpath.normpath(url)
    # posixpath.normpath always remove trailing slashes
    if url.endswith('/') and url != '/':
        new_url += '/'
    return new_url



def redirect(request, url, status=302):
    """Redirect client to relative or absolute ``url`` with ``status``.

    >>> @Request.application
    ... def test_app(request):
    ...     return redirect(request, request.args['redirect_to'],
    ...                     int(request.args.get('status', 302)))
    >>> client = werkzeug.Client(test_app)

    >>> client.get('/foo?redirect_to=../bar') # doctest: +ELLIPSIS
    (..., '302 FOUND', [...('Location', 'http://localhost/bar')...)

    >>> client.get('/foo?redirect_to=/') # doctest: +ELLIPSIS
    (..., '302 FOUND', [...('Location', 'http://localhost/')...)

    """
    return werkzeug.redirect(make_absolute_url(request, url), status)



class Request(werkzeug.Request):
    """TODO docstring."""
    def __init__(self, environ, session_secret_key=None):
        """TODO docstring"""
        super(Request, self).__init__(environ)
        self.session_secret_key = session_secret_key
        
    @werkzeug.cached_property
    def session(self):
        """TODO docstring."""
        return SecureCookie.load_cookie(
            self, secret_key=self.session_secret_key)



class Response(werkzeug.Response):
    """TODO docstring."""



class StaticFileResponse(Response):
    """Respond with the a static file.

    Respond with a static file, guessing the mimitype from the filename,
    and using WSGI’s ``file_wrapper`` when available.

    """
    
    def __init__(self, filename):
        """Create the response with the ``filename`` static file."""
        super(StaticFileResponse, self).__init__(filename)
        self.filename = filename
        self.file_obj = open(self.filename, 'rb')
        self.file_stat = os.stat(self.filename)
    
    def __call__(self, environ, start_response):
        """Return the file and set the response headers."""
        etag = '%s,%s,%s' % (self.filename.encode('utf-8'),
                             self.file_stat.st_size,
                             self.file_stat.st_mtime)
        etag = '"%s"' % hashlib.md5(etag).hexdigest()
        headers = [('Date', werkzeug.http_date()), ('Etag', etag)]
        # round to 1 second precision: no more than the HTTP header
        mtime = datetime.datetime.utcfromtimestamp(int(self.file_stat.st_mtime))
        if not werkzeug.is_resource_modified(environ, etag=etag,
                                             last_modified=mtime):
            start_response('304 Not Modified', headers)
            return []
       
        mime_type, encoding = mimetypes.guess_type(self.filename)
        headers.extend((
            ('Content-Type', mime_type or 'application/octet-stream'),
            ('Content-Length', str(self.file_stat.st_size)),
            ('Last-Modified', werkzeug. http_date(self.file_stat.st_mtime))))
        start_response('200 OK', headers)
        return werkzeug.wrap_file(environ, self.file_obj)


class KalamarSiteForKraken(kalamar.site.Site):
    def open_or_404(self, *args, **kwargs):
        try:
            return self.open(*args, **kwargs)
        except self.ObjectDoesNotExist:
            raise NotFound

def arg_count(function):
    """Return the nubmer of explicit arguments the function takes.
    
    *args and **kwargs arguments are excluded.
    
    >>> arg_count(lambda: 1)
    0
    >>> arg_count(lambda x, y: 1)
    2
    >>> arg_count(lambda x, y, *args: 1)
    2

    """
    args = inspect.getargspec(function)[0]
    return len(args)



def runserver(site, args=None):
    """Run a developpement server for the given Kraken ``site``.
    
    Setup test
    >>> real_argv = sys.argv
    >>> import logging
    >>> logging.getLogger('werkzeug').setLevel(logging.FATAL)

    Test
    >>> runserver(None, ['--help']) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    usage: ...
    >>> sys.argv = [sys.argv[0]]
    >>> try: runserver(None, ['--port=1']) # doctest: +ELLIPSIS
    ... except Exception, e: print e[1]
    Permission denied
    >>> sys.argv = [sys.argv[0], '--help']
    >>> runserver(None) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    usage: ...    

    Restore real argv
    >>> sys.argv = real_argv
    
    """
    # TODO manage other cases
    if not args:
        args = sys.argv[1:]
    if not args or args[0] != '--help':
        args = ['runserver'] + args
    action_runserver = werkzeug.script.make_runserver(
        lambda: site,extra_files=[]
        # Files for the reloader to watch
        # FIX ME: the config may not be in a single file
        #extra_files=[site.kalamar_site.config_filename]
        #if site and site.kalamar_site.config_filename else []
    )
    werkzeug.script.run(args=args)
