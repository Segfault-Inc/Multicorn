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
# along with Kalamar library.  If not, see <http://www.gnu.org/licenses/>.

import os
import sqlite3

def fill_sqlite_db(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE test ('key' NUMBER PRIMARY KEY, 'data' STRING);")
    cur.execute("INSERT INTO test (key, data) VALUES (1, 'test_data1');")
    cur.execute("INSERT INTO test (key, data) VALUES (2, 'test_data2');")
    cur.execute("INSERT INTO test (key, data) VALUES (3, 'test_data3');")
    cur.execute("INSERT INTO test (key, data) VALUES (4, 'test_data4');")
    cur.execute("INSERT INTO test (key, data) VALUES (5, 'test_data5');")
    conn.commit()
    cur.close()
