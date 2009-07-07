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
from werkzeug import Request, Response
from werkzeug.exceptions import HTTPException, NotFound

import kalamar
import koral


class Site(object):
    """
    Create a WSGI application from a site root and a kalamar configuration file.
    """
    
    def __init__(self, site_root, kalamar_conf=None):
        self.site_root = unicode(site_root)
        self.koral_site = koral.Site(site_root)
        self.kalamar_site = kalamar.Site(kalamar_conf)
    
    @Request.application
    def __call__(self, request):
        """WSGI entry point for every HTTP request"""
        try:
            template = self.find_template(request.path)
            if not template:
                raise NotFound
            template_name, extension, engine, remaining_path = template
        
            # Handle a simple template
            mimetype, encoding = mimetypes.guess_type('_.' + extension)
            values = {'request': request, 'remaining_path': remaining_path}
            content = self.koral_site.engines[engine].render(template_name,
                                                             values)
            return Response(content, mimetype=mimetype)
        except HTTPException, e:
            # e is also a WSGI application
            return e
    
    def handle_template(self, ):
        mimetype, encoding = mimetypes.guess_type('_.' + extension)
        values = {'request': request, 'remaining_path': remaining_path}
        content = self.koral_site.engines[engine].render(template, values)
        return Response(content, mimetype=mimetype)       
    
    def find_template(self, path):
        """
        Find the template for a given path (URL)
        
        Return (template_name, type_extension, engine, remaining_path)
        template_name is like <dirname>/<basename>.<type_extension>.<engine>
        with a path like <dirname>/<basename>/<remaining_path>
        
        If several templates could match the given path (try not to do that…),
        one of them is chosen arbitrarily.
        
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
                ipsum.svg.py

        >>> site.find_template(u'/')
        (u'index.html.genshi', u'html', u'genshi', u'')
        >>> site.find_template(u'/nonexistent')
        (u'index.html.genshi', u'html', u'genshi', u'nonexistent')
        
        >>> site.find_template(u'/hello/')
        (u'hello.html.py', u'html', u'py', u'')
        >>> site.find_template(u'/hello/world')
        (u'hello.html.py', u'html', u'py', u'world')
        
        >>> site.find_template(u'/lorem/')
        (u'lorem/index.txt.jinja2', u'txt', u'jinja2', u'')
        >>> site.find_template(u'/lorem/ipsum-dolor-sit-amet')
        (u'lorem/index.txt.jinja2', u'txt', u'jinja2', u'ipsum-dolor-sit-amet')
        >>> site.find_template(u'/lorem/ipsum')
        (u'lorem/ipsum.svg.py', u'svg', u'py', u'')
        >>> site.find_template(u'/lorem/ipsum/dolor/sit/amet')
        (u'lorem/ipsum.svg.py', u'svg', u'py', u'dolor/sit/amet')
        
        TODO: test more corner cases?
        """
        
        engines = [unicode(name) for name in self.koral_site.engines.keys()]
        
        def search_dir(dirname_parts, basename, remaining_parts):
            """
            Return (template_name, type_extension, engine) if a match is found
            or None
            
            Search for basename.*.* with the second * being a known koral engine
            """
            dirname = os.path.join(self.site_root, *dirname_parts)
            if not os.path.isdir(dirname):
                # dirname doesn’t exist or is a file
                return None

            basename += '.'
            for filename in os.listdir(dirname):
                if not filename.startswith(basename):
                    continue

                # what’s left after basename and the dot
                extension = filename[len(basename):]
                
                if '.' in extension:
                    type_extension, engine = extension.rsplit('.', 1)
                    if engine in engines:
                        # found a match
                        return (u'/'.join(dirname_parts + [filename]), 
                                type_extension,
                                engine,
                                u'/'.join(remaining_parts))

        path_parts = [part for part in path.split('/') if part]
        remaining_parts = collections.deque()
        
        while path_parts:        
            # Try the whole path as a directory and search for index.*
            result = search_dir(path_parts, 'index', remaining_parts)
            if result: return result
            
            # Try with the last part as a basename
            last_part = path_parts.pop()
            result = search_dir(path_parts, last_part, remaining_parts)
            if result: return result
            
            remaining_parts.appendleft(last_part)

        # Nothing matched, try index.* at the root of the site
        # Return None if there is no index.*
        return search_dir([], 'index', remaining_parts)


