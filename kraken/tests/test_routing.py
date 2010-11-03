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
Tests of kraken controllers using routing mechanism.

"""

from unittest import TestCase

from . import KrakenSiteMixin


# ``response`` objects have members such as ``status_code``
# pylint: disable=E1101

class TestRoutedRequests(KrakenSiteMixin, TestCase):
    """Suite testing routed requests."""
    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        super(TestRoutedRequests, self).setUp()
        from . import controllers
        self.test_app.register_controllers(controllers)
    # pylint: enable=C0103

    def test_message(self):
        """Test a static template."""
        response = self.client.get("/helloparam/world/")
        self._check_hello_template(response, "world")

    def test_message_redirect(self):
        """Test a template with redirection."""
        response = self.client.get("/helloparam/world")
        self.assertEqual(response.status_code, 301)
        self.assertEqual(
            response.headers["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(
            response.headers["Location"], "http://localhost/helloparam/world/")
        self.assert_("redirect" in response.data.lower())
        self.assert_("helloparam/world/" in response.data)

    def test_inexistent(self):
        """Test an unavailable template."""
        response = self.client.get("/bibopelula/")
        self.assertEqual(response.status_code, 404)

    def test_methods(self):
        """Test a template available with POST and GET methods."""
        response = self.client.get("/methods/")
        self._check_hello_template(response, "GET world")
        response = self.client.post("/methods/")
        self._check_hello_template(response, "POST world")

    def test_hello(self):
        """Test a template waiting for parameters."""
        response = self.client.get("/hello/?name=World")
        self._check_hello_template(response)

    def test_othertemplate(self):
        """Test a template called by a controller with a different name."""
        response = self.client.get("/another/template/World")
        self._check_hello_template(response)

    def test_weirdpath(self):
        """Test a template available with a weird path."""
        response = self.client.get("/World/message")
        self._check_hello_template(response)

    def test_simple_expose(self):
        """Test a controller without a template."""
        response = self.client.get("/simple_expose/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["Content-Type"], "text/plain; charset=utf-8")
        self.assertEqual(response.data, "Raw text from a controller ×")

# pylint: enable=E1101
