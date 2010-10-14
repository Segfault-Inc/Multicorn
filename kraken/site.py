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
Site
====

WSGI interface.

Instances of the Site class are WSGI applications. Create one for each
independent site with its own configuration.

"""

import datetime
import hashlib
import mimetypes
import os
import re
import sys
import types
import werkzeug
import werkzeug.contrib.securecookie
import werkzeug.wrappers


class Request(werkzeug.wrappers.Request):
    """Request object managing sessions with encrypted cookies."""
    def __init__(self, environ, session_secret_key=None):
        super(Request, self).__init__(environ)
        self.session_secret_key = session_secret_key

    @werkzeug.utils.cached_property
    def session(self):
        """Return the session."""
        return werkzeug.contrib.securecookie.SecureCookie.load_cookie(
            self, secret_key=self.session_secret_key)


class TemplateResponse(werkzeug.wrappers.Response):
    """Response serving a template."""
    def __init__(self, koral_site, path, values):
        template_suffix_re = ur"\.(.+)\.(%s)$" % u"|".join(
            re.escape(engine) for engine in koral_site.engines)

        searches = [(path, u"index")]
        # If path is empty (ie. path is u"" or u"/")
        # there is no path_parts[-1]
        if path:
            searches.append((os.path.dirname(path), os.path.basename(path)))

        template_name = None
        for dirname, basename in searches:
            abs_dirname = os.path.join(koral_site.path_to_root, dirname)
            if os.path.isdir(abs_dirname):
                for name in os.listdir(abs_dirname):
                    match = re.match(
                        re.escape(basename) + template_suffix_re, name)
                    if match:
                        template_name = u"/".join(
                            dirname.split(os.path.sep) + [name])
                        extension = match.group(1)
                        engine = match.group(2)

        if not template_name:
            raise werkzeug.exceptions.NotFound

        mimetype = mimetypes.guess_type(u"_." + str(extension))[0]
        content = koral_site.render(engine, template_name, values)
        super(TemplateResponse, self).__init__(content, mimetype=mimetype)


class StaticFileResponse(werkzeug.wrappers.Response):
    """Response serving a static file.

    Respond with a static file, guessing the mimitype from the filename,
    and using WSGI’s ``file_wrapper`` when available.

    """
    def __init__(self, filename):
        """Create the response with the ``filename`` static file."""
        super(StaticFileResponse, self).__init__(filename)

        if not os.path.isfile(filename):
            raise werkzeug.exceptions.NotFound

        self.filename = filename
        self.file_obj = open(self.filename, "rb")
        self.file_stat = os.stat(self.filename)

    def __call__(self, environ, start_response):
        """Return the file and set the response headers."""
        etag = "%s,%s,%s" % (
            self.filename.encode("utf-8"), self.file_stat.st_size,
            self.file_stat.st_mtime)
        # The hashlib module has a "md5" function
        # pylint: disable=E1101
        etag = '"%s"' % hashlib.md5(etag).hexdigest()
        # pylint: enable=E1101
        headers = [("Date", werkzeug.utils.http_date()), ("Etag", etag)]
        # round to 1 second precision: no more than the HTTP header
        mtime = datetime.datetime.utcfromtimestamp(int(self.file_stat.st_mtime))
        if not werkzeug.http.is_resource_modified(
            environ, etag=etag, last_modified=mtime):
            start_response("304 Not Modified", headers)
            return []

        mimetype, encoding = mimetypes.guess_type(self.filename)
        headers.extend((
                ("Content-Type", mimetype or "application/octet-stream"),
                ("Content-Length", str(self.file_stat.st_size)),
                ("Last-Modified", werkzeug.utils.http_date(
                        self.file_stat.st_mtime))))
        if encoding:
            headers.extend((
                    ("Content-Encoding", encoding),))
        start_response("200 OK", headers)
        return werkzeug.wsgi.wrap_file(environ, self.file_obj)


class Site(object):
    """WSGI application from a site root and a kalamar configuration file.

    :param site_root: Directory name of the root of the site.
    :param kalamar_site: Kalamar Site instance.
    :param koral_site: Koral Site instance.
    :param secret_key: String used for signed cookies for sessions. Use
        something like ``os.urandom(20)`` to get one.

    """
    def __init__(self, site_root, kalamar_site=None, koral_site=None,
                 secret_key=None):
        """Initialize the Site."""
        self.secret_key = secret_key
        self.site_root = os.path.expanduser(unicode(site_root))
        self.koral_site = koral_site
        self.kalamar_site = kalamar_site

        # Create a virtual package in sys.modules so that we can import
        # python modules in the site
        self.package_name = "kraken_site_%i" % id(self)
        module = types.ModuleType(self.package_name)
        module.__path__ = [self.site_root]
        module.kraken = self
        module.koral = self.koral_site
        module.kalamar = self.kalamar_site
        sys.modules[self.package_name] = module
    
    def __call__(self, environ, start_response):
        """WSGI entry point for every HTTP request."""
        request = Request(environ, self.secret_key)
        request.koral = self.koral_site
        request.kalamar = self.kalamar_site
        request.kraken = self
        path = os.path.join(*request.path.split(u"/")).strip(os.path.sep)

        try:
            if u"/." in request.path:
                # Hidden files and parent folders are forbidden
                raise werkzeug.exceptions.Forbidden

            if u"/__" in request.path:
                # Handle static file
                filename = os.path.join(self.site_root, path)
                response = StaticFileResponse(filename)
            else:
                # Handle template
                values = {
                    "request": request,
                    "import_": self.import_}
                response = TemplateResponse(self.koral_site, path, values)
                # We are sure that the response can be given, just check that we
                # have a trailing slash. If not, redirect the client.
                if not request.path.endswith(u"/"):
                    response = werkzeug.utils.append_slash_redirect(
                        request.environ)
        except werkzeug.exceptions.HTTPException, exception:
            # ``exception`` is also a WSGI application
            return exception(environ, start_response)

        if "session" in request.__dict__:
            request.session.save_cookie(response)

        return response(environ, start_response)
    
    def import_(self, name):
        """Helper for python controllers to "import" other controllers.

        Return a module object.

        >>> import kraken.tests
        >>> site = kraken.tests.make_site()
        >>> module = site.import_("inexistent")
        Traceback (most recent call last):
            ...
        ImportError: No module named inexistent

        >>> site.import_("lorem.ipsum") # doctest: +ELLIPSIS
        ...                             # doctest: +NORMALIZE_WHITESPACE
        <module 'kraken_site_....lorem.ipsum' 
            from '...kraken/tests/site/lorem/ipsum.py...'>

        """
        name = "%s.%s" % (self.package_name, name)
        # example: with name = "kraken_site_42.views.main", __import__(name)
        # returns the kraken_site_42 module with the views package
        # as an attribute, etc.
        module = __import__(name)
        for attr in name.split(".")[1:]:
            module = getattr(module, attr)
        return module
