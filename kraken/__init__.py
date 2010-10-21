# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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
Kraken - HTTP Request and Template Manager
==========================================

# TODO: write module documentation

"""

import datetime
import hashlib
import mimetypes
import os
import posixpath
import re
import sys
import urlparse
import werkzeug
import werkzeug.contrib.securecookie
import werkzeug.wrappers

from .site import Site


def make_absolute_url(request, url):
    """Return a clean absolute URL from ``request`` and ``url``.

    >>> # Fake request for http://localhost/foo/
    >>> import werkzeug
    >>> request = werkzeug.Request(werkzeug.create_environ(path="/foo/"))

    >>> # Various tests
    >>> make_absolute_url(request, "http://localhost/foo/bar/")
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, "/foo/bar/")
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, "./bar/")
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, "bar/")
    'http://localhost/foo/bar/'
    >>> make_absolute_url(request, "../bar/")
    'http://localhost/bar/'
    >>> make_absolute_url(request, "/")
    'http://localhost/'

    >>> # Same tests without the trailing slash
    >>> make_absolute_url(request, "http://localhost/foo/bar")
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, "/foo/bar")
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, "./bar")
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, "bar")
    'http://localhost/foo/bar'
    >>> make_absolute_url(request, "../bar")
    'http://localhost/bar'

    """
    # The object returned by urlparse has a "netloc" attribute
    # pylint: disable=E1101
    if urlparse.urlparse(url).netloc:
        # The URL has a "host" part: it’s already absolute
        return url
    # pylint: enable=E1101
    if not url.startswith("/"):
        # Relative to the current URL, not the site root
        path = request.base_url[len(request.host_url):]
        url = "/" + path + "/" + url
    new_url = request.host_url.rstrip("/") + posixpath.normpath(url)
    # posixpath.normpath always remove trailing slashes, add it if needed
    if url.endswith("/") and url != "/":
        new_url += "/"
    return new_url


def redirect(request, url, status=302):
    """Redirect client to relative or absolute ``url`` with ``status``.

    >>> # Create a client redirecting to the given "redirect_to" parameter
    >>> from . import site
    >>> @site.Request.application
    ... def test_app(request):
    ...     return redirect(request, request.args["redirect_to"],
    ...                     int(request.args.get("status", 302)))
    >>> client = werkzeug.Client(test_app)

    >>> # Check that the requests are redirected
    >>> client.get("/foo?redirect_to=../bar") # doctest: +ELLIPSIS
    (..., '302 FOUND', [...('Location', 'http://localhost/bar')...)
    >>> client.get("/foo?redirect_to=/") # doctest: +ELLIPSIS
    (..., '302 FOUND', [...('Location', 'http://localhost/')...)

    """
    return werkzeug.utils.redirect(make_absolute_url(request, url), status)


def runserver(site, args=None):
    """Run a developpement server for the given Kraken ``site``.

    >>> # Setup test
    >>> real_argv = sys.argv
    >>> import logging
    >>> logging.getLogger("werkzeug").setLevel(logging.FATAL)

    >>> # Test
    >>> runserver(None, ["--help"]) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    usage: ...
    >>> sys.argv = [sys.argv[0]]
    >>> try: runserver(None, ["--port=1"]) # doctest: +ELLIPSIS
    ... except Exception, e: print e[1]
    Permission denied
    >>> sys.argv = [sys.argv[0], "--help"]
    >>> runserver(None) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    usage: ...

    >>> # Restore real argv
    >>> sys.argv = real_argv

    """
    if not args:
        args = sys.argv[1:]
    if not args or args[0] != "--help":
        args = ["runserver"] + args
    # action_runserver is needed by Werkzeug, this is not useless
    # pylint: disable=W0612
    action_runserver = werkzeug.script.make_runserver(lambda: site)
    # pylint: enable=W0612
    werkzeug.script.run(args=args)
