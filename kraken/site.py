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
Instances of the Site class are WSGI applications.  Create one for each
independent site with it’s own configuration.
"""

import os.path
import collections
import mimetypes
import re
from werkzeug.exceptions import HTTPException, NotFound

import kalamar
import koral
from kraken import utils


class Site(object):
    """
    Create a WSGI application from a site root and a kalamar configuration file.
    """
    
    def __init__(self, site_root, kalamar_conf=None):
        self.site_root = unicode(site_root)
        self.koral_site = koral.Site(site_root)
        self.kalamar_site = kalamar.Site(kalamar_conf)
    
    @utils.Request.application
    def __call__(self, request):
        """WSGI entry point for every HTTP request"""
        try:
            if u'/__' in request.path:
                return self.handle_static_file(request)
            return self.handle_simple_template(request)
        except HTTPException, e:
            # e is also a WSGI application
            return e
    
    def handle_static_file(self, request):
        print request
        
    def handle_simple_template(self, request):
        """
        Try handling a request with only a template
        Return a Response object or raise NotFound
        """
        template = self.find_template(request.path)
        if not template:
            raise NotFound
        template_name, extension, engine = template
    
        # Handle a simple template
        mimetype, encoding = mimetypes.guess_type('_.' + extension)
        values = {'request': request}
        content = self.koral_site.engines[engine].render(template_name, values)
        return utils.Response(content, mimetype=mimetype)
            
    def find_template(self, path):
        """
        Search for an existing template named <path>/index.<type>.<engine>
        or <path>.<type>.<engine> where <engine> is a koral engine name.
        Return (template_name, type, engine) for the first one found or None.

        >>> import test.kraken
        >>> site = test.kraken.make_site()

        Directory stucture of site.site_root:
            index.html.genshi
            hello.html.py
            hello/
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
        path_parts = [part for part in path.split('/') if part]
        
        # Regular expression for .<type>.<engine> 
        # where <engine> is a koral engine name
        suffix_re = ur'\.(.+)\.(' + u'|'.join(re.escape(e) for e in
                                              self.koral_site.engines) + u')$'

        searches = [(path_parts, u'index' + suffix_re)]
        # if path_parts is empty (ie. path is u'' or u'/')
        # there is no path_parts[-1]
        if path_parts:
            searches.append((path_parts[:-1],
                             re.escape(path_parts[-1]) + suffix_re))

        for dir_parts, basename_re in searches:
            dirname = os.path.join(self.site_root, *dir_parts)
            if os.path.isdir(dirname):
                for name in os.listdir(dirname):
                    match = re.match(basename_re, name)
                    if match:
                        template = u'/'.join(dir_parts + [name])
                        return template, match.group(1), match.group(2)

