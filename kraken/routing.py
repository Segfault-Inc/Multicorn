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


from werkzeug.local import Local, LocalManager
from werkzeug.wrappers import Request
from werkzeug.exceptions import HTTPException
from werkzeug.routing import Map, Rule 
from .site import Site, TemplateResponse, StaticFileResponse
from functools import partial
import re
import os

local = Local()
local_manager = LocalManager([local])


url_map = Map()

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

    

def expose(rule=None, template=None, **kw):
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
        kw['endpoint'] = f
        url_map.add(Rule(rule, **kw))
        def template_renderer(values):
            return TemplateResponse(local('application').koral_site, template,
                    values)
        f.response = template_renderer
        return f
    return partial(decorate, rule, template)

class KrakenApplication(Site):
    """ A Kraken WSGI application.

    The application will have its url_maps filled with functions decorated with
    expose

    """

    

    def __init__(self, site_root, kalamar_site=None, 
            koral_site=None, secret_key=None, static_path="__static",
            static_url="static"):
        """
            TODO : copy the doc from kraken site and add static_path and
            static_url
        """
        local.application = self
        def get_path(request, path, **kwargs):
            filename = os.path.join(self.site_root, static_path,  path)
            return filename 
        get_path.response = lambda filename : StaticFileResponse(filename)
        url_map.add(Rule("/%s/<path:path>" % static_url, endpoint=get_path))
        super(KrakenApplication, self).__init__(site_root, kalamar_site, 
            koral_site, secret_key)

    def __call__(self, environ, start_response):
        local.application = self
        request = Request(environ, self.secret_key)
        local.kalamar = self.kalamar_site
        local.url_adapter = adapter = url_map.bind_to_environ(environ)
        try:
            handler, values = adapter.match()
            response = handler.response(handler(request, **values))
        except HTTPException, e:
            response = e
        return response(environ, start_response)

        
        


