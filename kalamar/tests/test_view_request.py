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
from kalamar.request import Request, ViewRequest
from kalamar.access_point.memory import Memory
from kalamar.config import Property
from kalamar.item import Item
from kalamar.site import Site


@nottest
def make_test_ap():
    return Memory({'id': int, 'name': str}, 'id')

@nottest
def make_test_second_ap():
    remote_prop = Property('remote', Item, relation='many-to-one', remote_ap='test_ap')
    return Memory({'id': int, 'label': str, 'remote': remote_prop},'id')

@nottest
def make_test_site():
    site = Site()
    site.register('test_ap',make_test_ap())
    site.register('test_remote_ap',make_test_second_ap())
    return site


def test_simple_view_request():
    """Create a simple view request, on a simgle access_point
    """
    ap = make_test_ap() 
    req = Request.parse({'id':3, 'name':'stuff'})
    aliases = {'id_select':'id', 'name_select':'name'}
    viewreq = ViewRequest(ap, aliases, req)
    #Assert that the aliases are all classified as 'manageable'
    eq_(viewreq.my_aliases, aliases)
    eq_(viewreq.aliases, aliases)
    eq_(viewreq.subviews, {})

def test_aliases_view_request():
    site = make_test_site()
    aliases = {'id_select': 'id', 'name_select': 'name', 'remote_select': 'remote.name'}
    req = Request.parse({'remote.name':'truc'})
    viewreq = ViewRequest(site.access_points['test_remote_ap'],aliases, req)
    eq_(viewreq.my_aliases, {'id_select': 'id', 'name_select': 'name'})
    eq_(viewreq.joins , {'remote':True})
    eq_(len(viewreq.subviews),1)
    subview = viewreq.subviews['remote']
    eq_(subview.my_aliases, {'remote_select':'name'})
    eq_(subview.subviews, {})
    

     

