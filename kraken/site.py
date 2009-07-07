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
        template, extension, engine, remaining_path = \
            self.find_template(request.path)
        mimetype, encoding = mimetypes.guess_type('_.' + extension)
        values = {'request': request, 'remaining_path': remaining_path}
        content = self.koral_site.engines[engine].render(template, values)
        return Response(content, mimetype=mimetype)
    
    def template_extensions(self):
        """Return the list of file extensions for templates."""
        return [unicode(name) for name in self.koral_site.engines.keys()]

    def find_template(self, path):
        """
        Find the template for a given path (URL)
        
        Return (template_name, type_extension, engine, remaining_path)
        template_name is like path/to/template.<type_extension>.<engine>
        
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
        
        def search_dir(path_parts, basename):
            """
            Return (template_name, type_extension, engine) if a match is found
            or None
            """
            # TODO: comment this
            dirname = os.path.join(self.site_root, *path_parts)
            if not os.path.isdir(dirname):
                return

            for filename in os.listdir(dirname):
                if not filename.startswith(basename + '.'):
                    continue

                extension = filename[len(basename) + 1:]
                for engine in self.template_extensions():
                    if extension.endswith('.' + engine):
                        return (u'/'.join(path_parts + [filename]), 
                                extension[:-(len(engine) + 1)],
                                engine)

        path_parts = [part for part in path.split('/') if part]
        remaining_parts = collections.deque()
        
        while path_parts:        
            # TODO: comment this
            result = search_dir(path_parts, 'index')
            if result:
                return result + (u'/'.join(remaining_parts),)
            
            basename = path_parts.pop()
            result = search_dir(path_parts, basename)
            if result:
                return result + (u'/'.join(remaining_parts),)
            
            remaining_parts.appendleft(basename)
        result = search_dir([], 'index')
        if result:
            return result + (u'/'.join(remaining_parts),)
        # TODO: this may be reached if there is no index.*  What should we do?


