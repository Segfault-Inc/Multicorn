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
Test the alchemy backend on an sqlite base
"""

from nose.tools import eq_, nottest
from kalamar.access_point.alchemy import AlchemyProperty,Alchemy
from kalamar.site import Site

from kalamar.tests.common import run_common, make_site

def make_ap():
    return make_testtable()

@run_common
def test_cache():
    return make_ap()

url = "sqlite:///"

@nottest
def make_testtable():
    id = AlchemyProperty(int, column_name='id')
    name = AlchemyProperty(unicode, column_name='name')
    ap = Alchemy(url,'test',{'id':id, 'name': name},'id',True)
    return ap


class TestAlchemy:

    def setUp(self):
        self.site = Site()
        self.site.register('test', make_testtable())
        self.items = []
        item = self.site.create('test', {'id': 1, 'name': u'Test'})
        self.items.append(item)
        item.save()
        item = self.site.create('test', {'id': 2, 'name': u'Test2'})
        self.items.append(item)
        item.save()

    def testsearch(self):
        items = list(self.site.search('test'))
        eq_(len(items), 2)
        items = list(self.site.search('test',{'id':1}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item['id'], 1)
        eq_(item['name'], 'Test')

    def testview(self):
        items = list(self.site.view('test', {'truc': 'id', 'name': u'name'}, 
                                    {}))
        eq_(len(items), 2)
        for item in items:
            assert 'truc' in item.keys() and 'name' in item.keys()
        items = list(self.site.view('test', {'truc': 'id', 'name': u'name'},
                     {'id': 1}))
        eq_(len(items), 1)

    def testupdate(self):
       item = self.site.open('test', {'id': 1})
       item['name'] = u'updated'
       item.save()
       item = self.site.open('test', {'id': 1})
       eq_(item['name'], u'updated')
    
    def tearDown(self):
        for item in self.items:
            item.delete()
        for ap in self.site.access_points.values():
            ap._table.drop()

