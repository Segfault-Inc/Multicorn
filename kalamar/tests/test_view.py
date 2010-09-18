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
Test the view request algorithm
"""

from nose.tools import eq_, nottest
from kalamar.request import Condition, Or, Request, ViewRequest
from kalamar.access_point.memory import Memory
from kalamar.property import Property
from kalamar.item import Item
from kalamar.site import Site


@nottest
def make_test_site():
    child_property = Property(Item, relation='one-to-many', remote_ap='level1', remote_property='parent')
    root_ap = Memory({'id' : Property(int), 'label': Property(str), 'children' : child_property},'id')

    parent_property = Property(Item, relation='many-to-one', remote_ap='root')
    child_property = Property(Item, relation='one-to-many', remote_ap='level2', remote_property='parent')
    level1_ap = Memory({'id' : Property(int),'label' : Property(str), 'parent' : parent_property, 'children' :
            child_property},'id')
    
    

    parent_property = Property(Item, relation='many-to-one', remote_ap='level1')
    level2_ap = Memory({'id' : Property(int),'label' : Property(str), 'parent' : parent_property},'id')
    site = Site()
    site.register('root', root_ap)
    site.register('level1', level1_ap)
    site.register('level2', level2_ap)
    return site

@nottest
def init_data():
    site = make_test_site()
    rootitem = site.create('root',{'label': 'root','id':0})
    rootitem.save()

    item1 = site.create('level1',{'label' : '1', 'id':1, 'parent' : rootitem})
    item1.save()
    item2 = site.create('level1',{'label' : '2', 'id':2, 'parent' : rootitem})
    item2.save()

    item11 = site.create('level2',{'label' : '1.1', 'id':1, 'parent' : item1})
    item11.save()
    item12 = site.create('level2',{'label' : '1.2', 'id':2, 'parent' : item1})
    item12.save()

    item21 = site.create('level2',{'label' : '2.1', 'id':3, 'parent' : item2})
    item21.save()
    item22 = site.create('level2',{'label' : '2.2', 'id':4, 'parent' : item2})
    item22.save()
    return site

def test_first_level():
    site = init_data()
    aliases = {'leaf_label' : 'children.label','root_label':'label'}
    items = list(site.view('root',aliases,{}))
    eq_(len(items), 2)
    conditions = {'children.label' : '1'}
    items = list(site.view('root',aliases,conditions))
    eq_(len(items), 1)


def test_leaf_nodes():
    site = init_data()
    aliases = {'leaf_label' : 'children.children.label', 'root_label': 'label','middle_label':'children.label'}
#    items = list(site.view('root',aliases,{}))
#    eq_(len(items), 4)
    condition = Condition('children.children.label' , "=" , '2.2')
    viewReq = ViewRequest(aliases,condition)
    items = list(site.view('root',aliases,condition))
    eq_(len(items), 1)
    assert(all([item['leaf_label'] == '2.2' for item in items]))
    condition = Condition('children.label' , "=", '2')
    items = list(site.view('root',aliases,condition))
    eq_(len(items), 2)
    assert(all([item['middle_label'] == '2' for item in items]))
    condition = {'children.label' : '2', 'children.children.label' : '1.1'}
    items = list(site.view('root',aliases,condition))
    eq_(len(items), 0)
    
def test_complex_condition():
    site = init_data()
    aliases = {'root_label' : 'label', 'middle_label': 'children.label'}
    condition = Or(Condition('children.label','=','1'), Condition('children.children.label','=','2.1'))
    items = list(site.view('root',aliases,condition))
    eq_(len(items), 3)
    



def test_many_to_ones():
    site = init_data()
    

    

