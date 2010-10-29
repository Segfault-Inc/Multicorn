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
Tests for kraken templates engines.

"""

import os.path
from nose.tools import eq_
from kraken.site import Site


def test_all_engines():
    """Test each supported kraken templates engine."""
    folder = os.path.dirname(__file__)
    site = Site(folder, os.path.join(folder, "templates"))

    def test_one_engine(engine):
        """Test one kraken templates engine."""
        if engine != "str-format":
            # str-format does not support default values
            response = site.render_template(
                engine, "hello.%s.html" % engine).strip()
            assert isinstance(response, unicode)
            eq_(response,
                u"<!DOCTYPE html>\n<html><body>Hello × World!</body></html>")
        response = site.render_template(
            engine, "hello.%s.html" % engine, {"name": "Python"}).strip()
        assert isinstance(response, unicode)
        eq_(response,
            u"<!DOCTYPE html>\n<html><body>Hello × Python!</body></html>")
         
    for engine in site.engines:
        yield test_one_engine, engine
