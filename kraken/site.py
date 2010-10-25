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
from werkzeug.exceptions import HTTPException
from werkzeug.routing import Map, Rule 
from functools import partial
from .template import BUILTIN_ENGINES


class Request(werkzeug.wrappers.Request):
    """Request object managing sessions with encrypted cookies."""
    def __init__(self, environ, session_secret_key=None):
        super(Request, self).__init__(environ)
        self.session_secret_key = session_secret_key

    @werkzeug.utils.cached_property
    def session(self):
        """Request session."""
        return werkzeug.contrib.securecookie.SecureCookie.load_cookie(
            self, secret_key=self.session_secret_key)


class TemplateResponse(werkzeug.wrappers.Response):
    """Response serving a template."""
    def __init__(self, site, path, values):
        template_suffix_re = ur"\.(.+)\.(%s)$" % u"|".join(
            re.escape(engine) for engine in site.engines)

        searches = [(path, u"index")]
        # If path is empty (ie. path is u"" or u"/")
        # there is no path_parts[-1]
        if path:
            searches.append((os.path.dirname(path), os.path.basename(path)))
        template_name = None
        for dirname, basename in searches:
            abs_dirname = os.path.join(site.template_root, dirname)
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
        content = site.render_template(engine, template_name, values)
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


def find_static_part(rule):
    """Return a possible template path from a rule.
    
    >>> find_static_part("/url/")
    '/url/'
    >>> find_static_part("/url/<string:id>")
    '/url/'
    >>> find_static_part("/url/<int:foo>/bar")
    '/url/foo/bar'
    >>> find_static_part("/url/<int:foo>/bar/<string:baz>")
    '/url/foo/bar/'

    """
    regexp = r'<\w+:(\w+)>(/\w+)'
    first_pass = re.sub(regexp, r'\1\2', rule)
    return re.sub(r'/<\w+:(\w+)>/?$', '/', first_pass)

    

def expose_template(rule=None, template=None, **kw):
    """ Decorator exposing a method as a template filler.

    The decorated function will be registered as an endpoint for the rule.
    
    :param rule:
        a werkzeug url path. If ommitted, will default to the function name
    :param template:
        a path to the template which should be filled with the dictionary
        returned by the decorated function. If ommitted, will default to the
        rule
    :param kw:
        keyword arguments passed directly to the werkzeug.routing.Rule.__init__
        method
        
        
    """
    def decorate(rule, template, f):
        """ Decorator that set a response attribute on a method to a
            TemplateResponse instance, initialized with the template
        """
        rule = rule or "/%s/" % f.__name__
        template = (template or find_static_part(rule)).strip(os.path.sep)
        def template_renderer(request, **kwargs):
            return TemplateResponse(f.krakensite, f.template,
                    f(request, **kwargs))
        kw['endpoint'] = template_renderer
        f.kw = kw
        f.template = template
        f.kraken_rule = rule
        return f
    return partial(decorate, rule, template)




class Site(object):
    """WSGI application from a site root and a kalamar configuration file.

    :param site_root: Root folder for the site.
    :param template_root: Root folder for the templates.
    :param kalamar_site: Kalamar Site instance.
    :param secret_key: String used for signed cookies for sessions. Use
        something like ``os.urandom(20)`` to get one.

    """
    def __init__(self, site_root, template_root, kalamar_site=None,
                 secret_key=None, static_url="static", static_path="__static",
                 fallback_on_template=True):
        """Initialize the Site."""
        self.secret_key = secret_key
        self.site_root = os.path.expanduser(unicode(site_root))
        self.template_root = os.path.expanduser(unicode(template_root))
        self.kalamar_site = kalamar_site
        self.engines = {}
        self.url_map = Map()
        self.static_path = static_path
        for name, engine_class in BUILTIN_ENGINES.items():
            self.register_engine(name, engine_class)
        # Create a virtual package in sys.modules so that we can import
        # python modules in the site
        self.package_name = "kraken_site_%i" % id(self)
        module = types.ModuleType(self.package_name)
        module.__path__ = [self.site_root]
        module.kraken = self
        module.kalamar = self.kalamar_site
        sys.modules[self.package_name] = module
        def get_path(request, path, **kwargs):
            filename = os.path.join(self.static_path,  path)
            if u"/.." in filename:
                raise werkzeug.exceptions.Forbidden
            return StaticFileResponse(filename)
        self.url_map.add(Rule("/%s/<path:path>" % static_url.strip("/"), endpoint=get_path))
        self.fallback_on_template = fallback_on_template


    def simple_template(self, request):
        path = request.path
        if u"../" in path or "/." in path:
            raise werkzeug.exceptions.Forbidden
        kwargs = {}
        kwargs['request'] = request
        kwargs['import_'] = self.import_
        response = TemplateResponse(self, "%s" %
            path.strip(os.path.sep),
            kwargs)
        if not request.path.endswith(u"/"):
            response = werkzeug.utils.append_slash_redirect(
                request.environ)
        return response

    def prehandle(self, request):
        """Dummy method called before trying to match the url"""
        pass
    
    def __call__(self, environ, start_response):
        """WSGI entry point for every HTTP request."""
        request = Request(environ, self.secret_key)
        adapter = self.url_map.bind_to_environ(environ)
        request.kraken = self
        request.kalamar = self.kalamar_site
        try:
            self.prehandle(request)
            handler, values = adapter.match()
            response = handler(request, **values)
        except werkzeug.exceptions.NotFound, e:
            if self.fallback_on_template:
                try:
                    response = self.simple_template(request)
                except HTTPException, e:
                    return e(environ, start_response)
            else:
                return e(environ, start_response)
        except HTTPException, e:
            return e(environ, start_response)
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


    def register_endpoint(self, function):
        """Registers function as an endpoint.
           The function must have an attribute "kraken_rule", defining the rule
           for werkzeug, and a "kw" attribute, defining the keywords arguments
           for the werkzeug rule.
        """
        function.krakensite = self
        self.url_map.add(Rule(function.kraken_rule, **function.kw))



    def register_controllers(self, module):
        """Register controllers from a module"""
        for attr in module.__dict__.values():
            if hasattr(attr, "__call__") and hasattr(attr, "kraken_rule"):
                self.register_endpoint(attr)

    def register_engine(self, name, engine_class):
        """Add a template engine to this site.
        
        :param name: Identifier string for this engine. Pass the same value
            to :meth:`render` to use the registered engine.
        :param engine_class: A concrete subclass of :class:`BaseEngine`.
        
        """
        self.engines[name] = engine_class(self.template_root)
    
    def render_template(self, site_engine, template_name, values=None,
                        lang=None, modifiers=None):
        """Shorthand to the engine render method."""
        return self.engines[site_engine].render(
            template_name, values or {}, lang, modifiers)
