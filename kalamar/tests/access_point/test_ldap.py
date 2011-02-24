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
Ldap test.

Test the Ldap access point.

"""
import ldap
from kalamar.access_point.ldap_ap import Ldap, LdapProperty

from ..common import make_site, run_common, require

ldap_test_hostname = "zero.fr"
ldap_test_path = "ou=test,dc=zero,dc=fr"
ldap_test_user = "cn=Manager,dc=zero,dc=fr"
ldap_test_password = "lol"

def make_ap():
    """Create a simple access point."""
    return Ldap(ldap_test_hostname, ldap_test_path, ldap_test_user, ldap_test_password, {
            "id": LdapProperty(int, "cn"),
            "name": LdapProperty(rdn_name="sn"),
            "objectClass": LdapProperty(auto=('top', 'person', 'inetOrgPerson'))})

def clean_ap(access_point):
    """Suppress all ldap objects in the test path"""
    ldapap = ldap.open(access_point.hostname)
    ldapap.simple_bind(ldap_test_user, ldap_test_password)
    for cn, _ in ldapap.search_s(access_point.ldap_path, ldap.SCOPE_ONELEVEL):
        ldapap.delete_s(cn)

def runner(test):
    """Test runner for ``test``."""
    access_point = make_ap()
    try:
        site = make_site(access_point,
            fill=not hasattr(test, "nofill"))
        test(site)
    finally:
        clean_ap(access_point)

@require("ldap")
@run_common
def test_common():
    """Launch common tests for ldap."""
    return None, runner, "Ldap"
