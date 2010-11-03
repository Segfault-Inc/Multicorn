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
Tests of kraken requests for simple templates (with no controllers).

"""

import os
from unittest import TestCase

from . import KrakenSiteMixin


# ``response`` objects have members such as ``status_code``
# pylint: disable=E1101

class TestSimpleRequests(KrakenSiteMixin, TestCase):
    """Suite testing simple requests."""
    def test_notfound(self):
        """Test an nonexistent template."""
        response = self.client.get("/nonexistent")
        self.assertEqual(response.status_code, 404)

    def test_hidden_notfound(self):
        """Test a nonexsistent template whose path is ``.*``."""
        response = self.client.get("/.hidden_but_nonexistent")
        self.assertEqual(response.status_code, 403)

    def test_hidden_template(self):
        """Test an exsistent template whose path is ``.*``."""
        response = self.client.get("/.hidden_template")
        self.assertEqual(response.status_code, 403)

    def test_static_notfound(self):
        """Test a nonexsistent static file."""
        response = self.client.get("/logo/inexistent.png")
        self.assertEqual(response.status_code, 404)

    def test_lipsum_notfound(self):
        """Test a nonexsistent template with ambiguous path.

        ``lorem/ipsum.txt.py`` exists, but not ``lorem/ipsum/dolor*``. This
        test is checking that ``lorem/ipsum/*`` paths do not call
        ``lorem/ipsum``.

        """
        response = self.client.get("/lorem/ipsum/dolor")
        self.assertEqual(response.status_code, 404)

    def test_index(self):
        """Test an index path."""
        response = self.client.get("/")
        self._check_hello_template(response, "root")

    def test_hello(self):
        """Test a template waiting for parameters."""
        response = self.client.get("/hello/?name=World")
        self._check_hello_template(response)

    def test_hello_redirect(self):
        """Test a template with redirection."""
        response = self.client.get("/hello?name")
        self.assertEqual(response.status_code, 301)
        self.assertEqual(
            response.headers["Location"], "http://localhost/hello/?name")
        self.assert_("redirect" in response.data.lower())
        self.assert_("hello/?name" in response.data)


class TestSession(KrakenSiteMixin, TestCase):
    """Suite testing sessions."""
    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        self.site_secret_key = os.urandom(20)
        super(TestSession, self).setUp()
    # pylint: enable=C0103

    def test_session(self):
        """Test a session by setting a value and checking its existence."""
        # Get the default value
        response = self.client.get("/session/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, "(no value)")

        # Set the value
        response = self.client.get("/session/?blah")
        self.assertEqual(response.status_code, 200)

        # Get again and check
        response = self.client.get("/session/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, "blah")

# pylint: enable=E1101
