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
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
View request test
=================

Test the view request algorithm.

"""

from nose.tools import eq_, nottest
from kalamar.access_point.memory import Memory
from kalamar.property import Property
from kalamar.site import Site


@nottest
def make_test_ap():
    """Build a Memory access point having a one-to-many relationship to another
    one"""
    one_to_many = Property(iter, relation='one-to-many', 
            remote_ap='test_remote_ap', remote_property='remote')
    return Memory({'id': Property(int), 'name': Property(unicode), 
        'manies': one_to_many}, 'id')

@nottest
def make_test_second_ap():
    """Build a Memory access point having a many-to-one relationship to another
    one"""
    remote_prop = Property(iter, relation='many-to-one', remote_ap='test_ap')
    return Memory({'id': Property(int),'label' : Property(unicode), 
        'remote': remote_prop},'id')



@nottest
def make_test_site():
    """Initializes the site with two access points, and populates it with test
    data"""
    site = Site()
    site.register('test_ap', make_test_ap())
    site.register('test_remote_ap', make_test_second_ap())
    my_item = site.create('test_ap', {'id':3, 'name': 'truc'})
    my_second_item = site.create('test_ap', {'id':10, 'name': 'truc'})
    my_item.save()
    my_second_item.save()
    remote = site.create('test_remote_ap', {
        'id' : 4 , 
        'label': 'remote_item', 
        'remote' : my_item})
    remote.save()
    remote2 = site.create('test_remote_ap', {
        'id' : 8 , 
        'label': 'remote_item2', 
        'remote' : my_second_item})
    remote2.save()
    my_item['manies'] = [remote, remote2]
    my_item.save()
    return site




def test_simplest_view():
    """Test simple view requests"""
    site = make_test_site()
    aliases = {'id_select': 'id', 
            'name_select': 'label', 
            'remote_select': 'remote.name'}
    req =  {'remote.id': 10}
    items = list(site.view("test_remote_ap", aliases, req))
    eq_(len(items), 1)
    uniq_item = items[0]
    eq_(uniq_item['name_select'],  'remote_item2')
    eq_(uniq_item['remote_select'],  'truc')
    eq_(uniq_item['id_select'],  8)
    
def test_one_to_many():
    """Test a one to many request"""
    site = make_test_site()
    aliases = {'local_id' : 'id',
            'local_name' : 'name',  
            'remote_label' : 'manies.label'}
    items = list(site.view('test_ap', aliases, {}))
    eq_(len(items), 3)

