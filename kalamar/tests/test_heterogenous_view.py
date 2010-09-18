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
Test an heterogeneous view on a memory and an alchemy ap
"""


from nose.tools import eq_, nottest
from kalamar.access_point.alchemy import AlchemyProperty,Alchemy
from kalamar.access_point.memory import Memory
from kalamar.site import Site
from kalamar.property import Property
from kalamar.item import Item


class TestHeterogeneous:

    def make_alchemy_ap(self):
        url = "sqlite:///"
        id = AlchemyProperty(int, column_name='id')
        label = AlchemyProperty(unicode, column_name='label')
        memory = AlchemyProperty(Item, column_name='memory', 
            relation='many-to-one', remote_ap='memory')
        ap = Alchemy(url, 'test', {'id': id, 'label': label, 'memory': memory},
            'id', True)
        return ap

    def make_memory_ap(self):
        ap = Memory({'id': Property(int), 'label': Property('str')}, 'id')
        return ap

    def setUp(self):
        self.site = Site()
        self.alchemy_ap = self.make_alchemy_ap()
        self.site.register('alchemy', self.alchemy_ap )
        self.site.register('memory', self.make_memory_ap())
        self.memitem = self.site.create('memory', 
            {'id': 1, 'label': u'memorytest'})
        self.memitem.save()
        self.dbitem = self.site.create('alchemy',
            {'id': 1, 'label': u'alchemytest', 'memory': self.memitem})
        self.dbitem.save()
        
    
    def test_view(self):
        pass
        items = list(self.site.view('alchemy',
            {'alch_id': u'id', 'alch_label': u'label',
             'mem_id': u'memory.id', 'mem_label': u'memory.label'}, {}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item['alch_id'],1)
        eq_(item['alch_label'], 'alchemytest')
        eq_(item['mem_id'],1)
        eq_(item['mem_label'],'memorytest')

    def tearDown(self):
        self.alchemy_ap._table.drop()
        

