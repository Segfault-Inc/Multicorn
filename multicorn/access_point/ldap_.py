# -*- coding: utf-8 -*-
# Copyright © 2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
LDAP
====

Access point storing items in an LDAP server.

"""

from __future__ import print_function
from multicorn.value import to_bytes
from multicorn.item import Item, MultiDict
from multicorn.access_point import AccessPoint
from multicorn.property import Property
from multicorn.request import Condition, And, Or, Not

try:
    import ldap
except ImportError:
    import sys
    print("WARNING: The LDAP AP is not available.", file=sys.stderr)
else:
    import ldap.modlist


class LdapItem(Item):
    """Item stored as a file."""
    @property
    def dn(self):
        """Distinguished name of the LDAP item."""
        return "cn=%s,%s" % (
            self[self.access_point.cn_name],  self.access_point.ldap_path)


class LdapProperty(Property):
    """Property for an LDAP access point."""
    def __init__(self, property_type=unicode, rdn_name=None, **kwargs):
        super(LdapProperty, self).__init__(property_type, **kwargs)
        self.rdn_name = rdn_name
        self.name = None


class Ldap(AccessPoint):
    """Access point to an LDAP server."""
    ItemClass = LdapItem

    def __init__(self, hostname, ldap_path, user, password, properties, 
                 identity_properties=None, encoding="utf-8"):
        for name, prop in properties.items():
            if not prop.rdn_name:
                prop.rdn_name = name
            if prop.rdn_name == "cn":
                self.cn_name = name
        if not self.cn_name:
            raise KeyError("Properties list must contains a 'cn'")
        elif not identity_properties:
            identity_properties = (self.cn_name,)
        super(Ldap, self).__init__(properties, identity_properties)
        self.encoding = encoding
        self.hostname = hostname
        self.ldap_path = ldap_path
        self.ldap = ldap.open(hostname)
        self.ldap.simple_bind(user, password)
    
    def _to_ldap_filter(self, condition):
        """Convert a multicorn condition to an LDAP filter."""
        if isinstance(condition, (And, Or, Not)):
            if isinstance(condition, Not):
                return "(! %s)" % self._to_ldap_filter(condition.sub_request)
            elif isinstance(condition, And):
                operator = "&"
            elif isinstance(condition, Or):
                operator = "|"
            return "(%s %s)" % (
                operator, " ".join(
                    self._to_ldap_filter(sub_condition) 
                    for sub_condition in condition.sub_requests))
        else:
            if condition.operator == "=":
                return "(%s%s%s)" % (
                    self.properties[condition.property.name].rdn_name,
                    condition.operator, condition.value)
            else:
                # No way to create a LDAP filter when we have no equalities
                raise ValueError

    def search(self, request):
        try:
            ldap_request = self._to_ldap_filter(request)
        except ValueError:
            # No LDAP filter can be created, use a software filter
            ldap_request = None

        for _, ldap_result in self.ldap.search_s(
            self.ldap_path, ldap.SCOPE_ONELEVEL, ldap_request or "objectClass=*",
            # Restrict results to declared properties:
            [prop.rdn_name for prop in self.properties.values()]):
            multidict = MultiDict()
            for prop in self.properties.values():
                if prop.relation != "one-to-many":
                    values = (
                        value.decode(self.encoding)
                        for value in ldap_result.get(prop.rdn_name, ()))
                    multidict.setlist(prop.name, tuple(values) or (None,))
            item = self.create(multidict)
            if ldap_request or request.test(item):
                item.saved = True
                yield item
                    
    def delete(self, item):
        self.ldap.delete_s(item.dn)



    def save(self, item):
        modifications = {}
        for key in item:
            rdn_key = self.properties[key].rdn_name
            if self.properties[key].relation != "one-to-many":
                if item.getlist(key) != (None,):
                    modifications[rdn_key] = tuple(
                        to_bytes(value, self.encoding) for value in item.getlist(key))
                
        old_entry = self.open(
            Condition(self.cn_name, "=", item[self.cn_name]), None)
        if old_entry:
            # Here we replace properties names in order to make the diff
            old_rdn_entry = {}
            for key in old_entry:
                old_rdn_entry[self.properties[key].rdn_name] = tuple(
                    to_bytes(value, self.encoding)
                    for value in old_entry.getlist(key))
            self.ldap.modify_s(
                item.dn, ldap.modlist.modifyModlist(
                    old_rdn_entry, modifications))
        else:
            self.ldap.add_s(item.dn, modifications.items())
        item.saved = True
