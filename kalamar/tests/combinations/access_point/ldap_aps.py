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
Tests for Ldap access point combinations.

"""

from kalamar.item import Item
from kalamar.access_point.ldap_ import Ldap, LdapProperty

from ..test_combinations import FirstAP, SecondAP
from ...common import require
from ...access_point.test_ldap import clean_ap


HOSTNAME = "localhost"
PATH = "ou=test,dc=test,dc=fr"
PATH2 = "ou=test2,dc=test,dc=fr"
USER = "cn=Manager,dc=test,dc=fr"
PASSWORD = "password"


@FirstAP(teardown=clean_ap)
@require("ldap")
def make_first_ap():
    """First access point for Ldap."""
    return Ldap(HOSTNAME, PATH, USER, PASSWORD, {
            "id": LdapProperty(int, "cn"),
            "name": LdapProperty(rdn_name="sn"),
            "color": LdapProperty(rdn_name="title"),
            "second_ap": LdapProperty(
                Item, rdn_name="givenName", relation="many-to-one", remote_ap="second_ap",
                remote_property="code"),
            "objectClass": LdapProperty(auto=('top', 'person', 'inetOrgPerson'))})

@SecondAP(teardown=clean_ap)
@require("ldap")
def make_second_ap():
    """Second access point for Ldap."""
    return Ldap(HOSTNAME, PATH2, USER, PASSWORD, {
            "code": LdapProperty(rdn_name="cn"),
            "name": LdapProperty(rdn_name="sn"),
            "first_aps": LdapProperty(
                iter, rdn_name="givenName", relation="one-to-many", remote_ap="first_ap",
                remote_property="second_ap"),    
            "objectClass": LdapProperty(auto=('top', 'person', 'inetOrgPerson'))})
