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
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
PostgreSQL tests.

"""

import os
import sys
import warnings
from unittest2 import TestCase

from _database import TestSite, site_tests
from test.kalamar import Site

try:
    import pg8000
except ImportError:
    warnings.warn('PostgresSQL access not tested (could not import pg8000)',
                  ImportWarning)
else:
    site = Site(os.path.join(os.path.dirname(__file__),
                             'data', 'kalamar_postgres.conf'))

    class TestSite(TestSite): site = site

    try:
        site.access_points.values()[0].get_connection()
    except Exception, e:
        warnings.warn('PostgresSQL access not tested (%s)' % unicode(e))
    else:
        for access_point in site.access_points:
            for test in site_tests:
                cls = type('%s_%s' % (test.__name__, access_point),
                           (TestSite, test, TestCase),
                           {'access_point_name': access_point})
                setattr(sys.modules[__name__], cls.__name__, cls)
