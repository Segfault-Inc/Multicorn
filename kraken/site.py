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

import mimetypes
import re
import os
import sys
import types
import werkzeug
import werkzeug.contrib.securecookie
import werkzeug.wrappers
from werkzeug.exceptions import HTTPException
from werkzeug.routing import Map, Rule 
from functools import partial
from .template import BUILTIN_ENGINES, find_template


def _find_static_part(rule):
    """Return a possible template path from a rule.
    
    >>> _find_static_part("/url/")
    '/url/'
    >>> _find_static_part("/url/<string:id>")
    '/url/'
    >>> _find_static_part("/url/<int:foo>/bar")
    '/url/foo/bar'
    >>> _find_static_part("/url/<int:foo>/bar/<string:baz>")
    '/url/foo/bar/'

    """
    regexp = r"<\w+:(\w+)>(/\w+)"
    first_pass = re.sub(regexp, r"\1\2", rule)
    return re.sub(r"/<\w+:(\w+)>/?$", "/", first_pass)


def expose_template(rule=None, template=None, **kwargs):
    """Decorator exposing a method as a template filler.

    The decorated function will be registered as an endpoint for the rule.
    
    :param rule: Werkzeug url path. If ommitted, will default to the function
        name.
    :param template: Path to the template which should be filled with the
        dictionary returned by the decorated function. If ommitted, will
        default to the rule.
    :param kwargs: Keyword arguments passed directly to
        :meth:`werkzeug.routing.Rule.__init__`.
        
    """
    def decorate(rule, template, function):
        """Decorator marking a function to be registered as a template."""
        function = expose(rule, **kwargs)(function)
        template = template or _find_static_part(function.kraken_rule)
        function.template_path = template.strip(os.path.sep)
        return function
    return partial(decorate, rule, template)


def expose(rule=None, **kwargs):
    """Decorator marking a method as to be exposed to the site."""
    def decorate(rule, function):
        """Decorator marking a function to be registered."""
        rule = rule or "/%s/" % function.__name__
        function.kwargs = kwargs
        function.kraken_rule = rule
        return function
    return partial(decorate, rule)


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
        template = find_template(path, site.engines, site.template_root)
        if template:
            template_name, extension, engine = template
            mimetype = mimetypes.guess_type(u"_." + str(extension))[0]
            content = site.render_template(engine, template_name, values)
            super(TemplateResponse, self).__init__(content, mimetype=mimetype)
        else:
            raise werkzeug.exceptions.NotFound


class ControllerResponse(werkzeug.wrappers.Response):
    """Response serving an annotated function using its template."""
    def __init__(self, site, function, request, **kwargs):
        values = function(request, **kwargs)
        template_name, extension, engine = function.template
        mimetype = mimetypes.guess_type(u"_." + str(extension))[0]
        content = site.render_template(engine, template_name, values)
        super(ControllerResponse, self).__init__(content, mimetype=mimetype)


class Site(object):
    """WSGI application from a site root and a kalamar configuration file.

    :param site_root: Root folder for the site.
    :param template_root: Root folder for the templates.
    :param kalamar_site: Kalamar Site instance.
    :param secret_key: String used for signed cookies for sessions. Use
        something like ``os.urandom(20)`` to get one.

    """
    def __init__(self, site_root=".", template_root="views", kalamar_site=None,
                 secret_key=None, fallback_on_template=True):
        """Initialize the Site."""
        root = os.path.dirname(__file__) if vars().has_key("__file__") else "."
        self.site_root = os.path.join(root, site_root)
        self.template_root = os.path.join(root, template_root)
        self.kalamar_site = kalamar_site
        self.secret_key = secret_key
        self.fallback_on_template = fallback_on_template

        self.engines = {}
        self.url_map = Map()
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
    
    def __call__(self, environ, start_response):
        """WSGI entry point for every HTTP request."""
        request = Request(environ, self.secret_key)
        sites = {"kraken": self, "kalamar": self.kalamar_site}

        # Find an endpoint for the request
        try:
            response = self.prehandle(request)
            if not response:
                adapter = self.url_map.bind_to_environ(environ)
                handler, values = adapter.match()
                values.update(sites)
                response = handler(request, **values)
        except werkzeug.exceptions.NotFound, exception:
            if self.fallback_on_template:
                try:
                    response = self.simple_template(request, **sites)
                except HTTPException, exception:
                    return exception(environ, start_response)
            else:
                return exception(environ, start_response)
        except HTTPException, exception:
            return exception(environ, start_response)

        # Add session cookie if needed
        if "session" in request.__dict__:
            request.session.save_cookie(response)

        return response(environ, start_response)

    def prehandle(self, request):
        """Method called before trying to match the url.

        Can return a ``response`` object prevailing against default url map.

        """

    def simple_template(self, request, **kwargs):
        """Serve a template corresponding to ``request``."""
        path = request.path
        if u"../" in path or "/." in path:
            raise werkzeug.exceptions.Forbidden
        kwargs["request"] = request
        kwargs["import_"] = self.import_
        response = TemplateResponse(self, path.strip(os.path.sep), kwargs)
        if not request.path.endswith(u"/"):
            response = werkzeug.utils.append_slash_redirect(request.environ)
        return response
    
    def render_template(self, site_engine, template_name, values=None,
                        lang=None, modifiers=None):
        """Shorthand to the engine render method."""
        return self.engines[site_engine].render(
            template_name, values or {}, lang, modifiers)

    def import_(self, name):
        """Helper for python controllers to "import" other controllers.

        Return a module object.

        >>> import kraken.tests
        >>> site = kraken.tests.make_site()
        >>> module = site.import_("inexistent")
        Traceback (most recent call last):
            ...
        ImportError: No module named inexistent
        >>> lipsum = site.import_("lorem.ipsum")
        >>> lipsum.render(None).data # doctest: +ELLIPSIS
        ...                          # doctest: +NORMALIZE_WHITESPACE
        '\\n Lorem ipsum dolor...'

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
        """Register ``function`` as an endpoint.

        ``function`` must have three attributes:

        - ``kraken_rule`` defining the rule for werkzeug;
        - ``kwargs`` defining the keywords arguments for the werkzeug rule;
        - ``template`` defining the relative path to the template.

        """
        if hasattr(function, "template_path"):
            function.template = find_template(
                function.template_path, self.engines, self.template_root)
            if function.template is None:
                raise RuntimeError(
                    "The template '%s' used by function '%s' doesn't exist" % (
                        function.template_path, function.__name__))
            function.kwargs["endpoint"] = partial(
                ControllerResponse, self, function)
        else:
            function.kwargs["endpoint"] = function
        self.url_map.add(Rule(function.kraken_rule, **function.kwargs))

    def register_controllers(self, module):
        """Register controllers from ``module``."""
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
