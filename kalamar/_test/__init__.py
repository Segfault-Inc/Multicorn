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
Testing module for kalamar, only used for testing.

"""

import os
import sqlite3

def fill_sqlite_db(connection):
    """Fill sqlite database with test values."""
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE test ('key' NUMBER PRIMARY KEY, 'data' STRING);")
    cursor.execute("INSERT INTO test (key, data) VALUES (1, 'test_data1');")
    cursor.execute("INSERT INTO test (key, data) VALUES (2, 'test_data2');")
    cursor.execute("INSERT INTO test (key, data) VALUES (3, 'test_data3');")
    cursor.execute("INSERT INTO test (key, data) VALUES (4, 'test_data4');")
    cursor.execute("INSERT INTO test (key, data) VALUES (5, 'test_data5');")
    connection.commit()
    cursor.close()
