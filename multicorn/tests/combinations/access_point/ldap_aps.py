# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for Ldap access point combinations.

"""

from multicorn.item import Item
from multicorn.access_point.ldap_ import Ldap, LdapProperty

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
