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
WSGI interface.

Instances of the Site class are WSGI applications. Create one for each
independent site with its own configuration.

"""

import os
import sys
import collections
import mimetypes
import types
import re
import warnings
import werkzeug
from werkzeug.exceptions import HTTPException, NotFound, Forbidden

import koral
import kalamar
from kraken import utils



class Site(object):
    """WSGI application from a site root and a kalamar configuration file."""
    def __init__(self, site_root, kalamar_site=None, secret_key=None):
        """Initialize the Site.

        ``site_root``: dirname of the root of the site
        ``kalamar_conf``: path to the kalamar config file
        ``secret_key``: used for signed cookies for sessions
                        use something like os.urandom(20) to get one

        """
        self.secret_key = secret_key
        self.site_root = os.path.expanduser(unicode(site_root))
        self.koral_site = koral.Site(self.site_root)
        self.kalamar_site = kalamar_site
        
        # create a virtual package in sys.modules so that we can import 
        # python modules in the site
        self.package_name = 'kraken_site_%i' % id(self)
        module = types.ModuleType(self.package_name)
        module.__path__ = [self.site_root]
        module.kraken = self
        module.koral = self.koral_site
        module.kalamar = self.kalamar_site
        sys.modules[self.package_name] = module
    
    def __call__(self, environ, start_response):
        """WSGI entry point for every HTTP request."""
        request = self.make_request(environ)
        try:
            response = self.handle_request(request)
        except HTTPException, exception:
            # e is also a WSGI application
            return exception(environ, start_response)

        response = self.process_response(request, response)
        return response(environ, start_response)
    
    def make_request(self, environ):
        """TODO docstring."""
        request = utils.Request(environ, self.secret_key)
        request.koral = self.koral_site
        request.kalamar = self.kalamar_site
        request.kraken = self
        request.template_response = lambda *args, **kwargs:\
            self.template_response(request, *args, **kwargs)
        return request
    
    def handle_request(self, request):
        """TODO docstring."""
        response = None
        if u'/__' in request.path:
            try:
                response = self.handle_static_file(request)
            except NotFound:
                pass
        if not response:
            try:
                response = self.handle_simple_template(request)
            except NotFound:
                response = self.handle_python(request)
        return response

    def process_response(self, request, response):
        """TODO docstring."""
        # utils.Request.session is a werkzeug.cached_property
        # the actual session object is in request.__dict__ if
        # request.session has been accessed at least once.
        # If it is not there, the session hasn’t changed:
        # no need to set the cookie.
        if 'session' in request.__dict__:
            request.session.save_cookie(response)
            
        return response

    @staticmethod
    def handle_trailing_slash(request):
        """Redirect if ``request.path`` has no trailing slash."""    
        if not request.path.endswith(u'/'):
            werkzeug.append_slash_redirect(request.environ)
        
    def handle_static_file(self, request):
        """Try handling a request with a static file.

        The request path is interpreted as a filename relative to the site root.
        Return a Response object or raise NotFound.

        """
        filename = os.path.join(self.site_root, *(
            part for part in request.path.split(u'/')
            if part and part != u'..'))
        if not os.path.isfile(filename):
            raise NotFound
        if u'/.' in request.path:
            raise Forbidden
        return utils.StaticFileResponse(filename)
        
    def handle_python(self, request):
        """Try handling a ``request`` with a python controller.

        Return a Response object or raise NotFound."""
        parts = (part for part in request.path.replace('.', '_').split(u'/')
                      if part and part != u'..')
        name = '.'.join(parts)
        try:
            module = self.import_(name)
        except ImportError:
            raise NotFound
        if hasattr(module, 'handle_request'):
            self.handle_trailing_slash(request)
            return module.handle_request(request)
        raise NotFound
        
    def handle_simple_template(self, request):
        """Try handling a request with only a template.
        
        Return a Response object or raise NotFound.

        """
        template = self.find_template(request.path)
        if not template:
            raise NotFound
        if u'/.' in request.path:
            raise Forbidden
        # only if the template exists
        # (ie the redirect doesn’t lead to a "404 Not Found")
        self.handle_trailing_slash(request)
        template_name, extension, engine = template
        return self.template_response(request, template_name, {}, extension,
                                      engine)
    
    def template_response(self, request, template_name, values=None,
                          extension=None, engine=None):
        """Build a response for ``request`` according to given parameters.

        >>> import kraken.tests
        >>> site = kraken.tests.make_site()
        >>> req = site.make_request({})
        >>> site.template_response(req, 'foo')
        Traceback (most recent call last):
            ...
        ValueError: extension and engine not provided but template_name does not match *.<extension>.<engine>
        >>> site.template_response(req, 'foo', {}, 'html')
        Traceback (most recent call last):
            ...
        TypeError: Can provide both of extension and engine or neither, but not only one
        >>> response = site.template_response(req, 'index.html.genshi')
        >>> response.mimetype
        'text/html'
        >>> response = site.template_response(req, 'index.html.genshi', {},
        ...                                   'html', 'genshi')
        >>> response.mimetype
        'text/html'

        """
        if extension and engine:
            pass # exclude this case from the following else clauses
        elif extension or engine:
            raise TypeError('Can provide both of extension and engine '
                            'or neither, but not only one')
        else:
            match = re.match(u'^.+' + self.template_suffix_re, template_name)
            if not match:
                raise ValueError('extension and engine not provided but '
                                 'template_name does not match '
                                 '*.<extension>.<engine>')
            extension = match.group(1)
            engine = match.group(2)
        
        # Handle a simple template
        mimetype = mimetypes.guess_type(u'_.' + str(extension))[0]
        if not values:
            values = {}
        values.update(self.simple_template_context(request))
        content = self.koral_site.render(engine, template_name, values)
        return utils.Response(content, mimetype=mimetype)
    
    def simple_template_context(self, request):
        """TODO docstring."""
        class FakeSite:
            """TODO docstring."""

        site = FakeSite()
        site.kalamar = request.kalamar
        site.koral = self.koral_site
        site.kraken = self
        def import_(name):
            warnings.warn('Using import_ from the site is deprecated. It may '
                          'be removed for template contexts in the future.',
                          DeprecationWarning,
                          stacklevel=2)
            return self.import_(name)
        return dict(request=request, site=site, import_=import_)
    
    def import_(self, name):
        """Helper for python controllers to "import" other controllers.

        Return a module object.

        >>> import kraken.tests
        >>> site = kraken.tests.make_site()
        >>> module = site.import_('inexistent')
        Traceback (most recent call last):
            ...
        ImportError: No module named inexistent

        >>> site.import_('lorem.ipsum') # doctest: +ELLIPSIS
        ...                             # doctest: +NORMALIZE_WHITESPACE
        <module 'kraken_site_....lorem.ipsum' 
            from '...kraken/tests/site/lorem/ipsum.py...'>
        """
        name = self.package_name + '.' + name
        # example: with name = 'kraken_site_42.views.main', __import__(name)
        # returns the kraken_site_42 module with the views package
        # as an attribute, etc.
        module = __import__(name)
        for attr in name.split('.')[1:]:
            module = getattr(module, attr)
        return module
        
        
    def find_template(self, path):
        """Find the template at ``path``.

        Search for an existing template named <path>/index.<type>.<engine>
        or <path>.<type>.<engine> where <engine> is a koral engine name.

        Return (template_name, type, engine) for the first one found or None.

        >>> import kraken.tests
        >>> site = kraken.tests.make_site()

        Directory stucture of site.site_root:
            index.html.genshi
            hello.html.jinja2
            hello/
                index.genshi # No <type>
                index.html # No <engine>
                index.html.foo # Non-existent <engine>
            lorem/
                index.txt.jinja2

        >>> site.find_template(u'/')
        (u'index.html.genshi', u'html', u'genshi')
        >>> site.find_template(u'/nonexistent')
        
        >>> site.find_template(u'/hello/')
        (u'hello.html.jinja2', u'html', u'jinja2')
        >>> site.find_template(u'/hello/world')
        
        >>> site.find_template(u'/lorem/')
        (u'lorem/index.txt.jinja2', u'txt', u'jinja2')
        >>> site.find_template(u'/lorem/ipsum')

        """
        path_parts = [part for part in path.split(u'/')
                      if part and part != u'..']
        
        searches = [(path_parts, u'index' + self.template_suffix_re)]
        # if path_parts is empty (ie. path is u'' or u'/')
        # there is no path_parts[-1]
        if path_parts:
            searches.append((path_parts[:-1], re.escape(path_parts[-1]) +
                                              self.template_suffix_re))

        for dir_parts, basename_re in searches:
            dirname = os.path.join(self.koral_site.path_to_root, *dir_parts)
            if os.path.isdir(dirname):
                for name in os.listdir(dirname):
                    match = re.match(basename_re, name)
                    if match:
                        template = u'/'.join(dir_parts + [name])
                        return template, match.group(1), match.group(2)
    
    @werkzeug.cached_property
    def template_suffix_re(self):
        """TODO docstring."""
        # Regular expression for .<type>.<engine> 
        # where <engine> is a koral engine name
        return ur'\.(.+)\.(' + u'|'.join(re.escape(e) for e in
                                         self.koral_site.engines) + u')$'

    
