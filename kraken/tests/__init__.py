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
Kraken test suite.

"""

import os.path
import werkzeug
import werkzeug.test


def make_site(secret_key=None):
    """Create a kraken site with ``"./site"`` as root path."""
    # Import kraken here so that coverage sees module-level statements
    import kraken
    root = os.path.join(os.path.dirname(__file__), "site")
    return kraken.site.Site(root, root, None, secret_key)


class KrakenSiteMixin(object):
    """Mixin class that setup kraken sites."""
    test_app = None

    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        """Create a ``Client`` that simulates HTTP requests.

        See http://werkzeug.pocoo.org/documentation/0.5/test.html

        """
        if self.__class__.test_app is None:
            self.__class__.test_app = make_site(
                secret_key=getattr(self, "site_secret_key", None))
        self.client = werkzeug.test.Client(
            self.test_app, werkzeug.wrappers.BaseResponse)

    def tearDown(self):
        """Remove the client from the class and clean the URL map."""
        from kraken import site
        self.__class__.test_app = None
        site.url_map = werkzeug.routing.Map()
    # pylint: enable=C0103

    def _check_hello_template(self, response, message="World"):
        """Check that response is 200 with good html utf-8 content.

        :param str message: word that should be included in the variable hello
            template string.

        """
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(
            response.data, "<html><body>Hello × %s!</body></html>\n" % message)
