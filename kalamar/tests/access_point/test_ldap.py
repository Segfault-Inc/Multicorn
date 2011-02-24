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

from kalamar.access_point.ldap_ import Ldap, LdapProperty

from ..common import make_site, run_common, require

HOSTNAME = "localhost"
PATH = "ou=test,dc=test,dc=fr"
USER = "cn=Manager,dc=test,dc=fr"
PASSWORD = "password"


def make_ap():
    """Create a simple access point."""
    return Ldap(HOSTNAME, PATH, USER, PASSWORD, {
            "id": LdapProperty("cn", int),
            "name": LdapProperty("sn"),
            "objectClass": LdapProperty(
                auto=('top', 'person', 'inetOrgPerson'))})

def clean_ap():
    """Suppress all ldap objects in the test path."""
    import ldap

    ldapap = ldap.open(HOSTNAME)
    ldapap.simple_bind(USER, PASSWORD)
    for cn, _ in ldapap.search_s(PATH, ldap.SCOPE_ONELEVEL):
        ldapap.delete_s(cn)

def runner(test):
    """Test runner for ``test``."""
    access_point = make_ap()
    try:
        site = make_site(access_point, fill=not hasattr(test, "nofill"))
        test(site)
    finally:
        clean_ap()

@require("ldap")
@run_common
def test_common():
    """Launch common tests for LDAP."""
    return None, runner, "Ldap"
