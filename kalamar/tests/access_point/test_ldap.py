# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

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
            "id": LdapProperty(int, "cn"),
            "name": LdapProperty(rdn_name="sn"),
            "objectClass": LdapProperty(
                auto=('top', 'person', 'inetOrgPerson'))})

def clean_ap(access_point):
    """Suppress all ldap objects in the test path."""
    import ldap

    ldapap = ldap.open(access_point.hostname)
    ldapap.simple_bind(USER, PASSWORD)
    for cn, _ in ldapap.search_s(access_point.ldap_path, ldap.SCOPE_ONELEVEL):
        ldapap.delete_s(cn)

def runner(test):
    """Test runner for ``test``."""
    access_point = make_ap()
    try:
        site = make_site(access_point, fill=not hasattr(test, "nofill"))
        test(site)
    finally:
        clean_ap(access_point)

@require("ldap")
@run_common
def test_common():
    """Launch common tests for LDAP."""
    return None, runner, "Ldap"
