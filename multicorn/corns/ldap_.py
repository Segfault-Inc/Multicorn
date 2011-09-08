from __future__ import print_function
from multicorn.requests.types import Type, List
from multicorn.utils import colorize
from multicorn.requests import CONTEXT as c
from multicorn.item.base import BaseItem
from .easy import EasyCorn


try:
    import ldap
except ImportError:
    import sys
    print(colorize(
        'yellow',
        "WARNING: The LDAP AP is not available."), file=sys.stderr)
else:
    import ldap.modlist


class LdapItem(BaseItem):

    @property
    def dn(self):
        if not self.get("cn", False):
            raise ValueError("Ldap items must have a cn")
        return "cn=%s,%s" % (self['cn'][0], self.corn.path)


class Ldap(EasyCorn):

    Item = LdapItem

    def __init__(self, name, hostname, path, user=None,
                 password=None, encoding="utf-8", identity_properties=("cn",),
                 objectClass='inetOrgPerson'):
        super(Ldap, self).__init__(name, identity_properties)
        self.register("cn")
        self.encoding = encoding
        self.hostname = hostname
        self.path = path
        self.objectClass = objectClass
        self.user = user
        self.password = password

    def bind(self, multicorn):
        super(Ldap, self).bind(multicorn)
        if not hasattr(self.multicorn, '_ldap_metadatas'):
            self.multicorn._ldap_metadatas = {}
        connect_point = "%s@%s" % (self.user, self.hostname)
        connection = self.multicorn._ldap_metadatas.get(connect_point, None)
        if connection is None:
            connection = ldap.open(self.hostname)
            if self.user and self.password:
                connection.simple_bind_s(self.user, self.password)
            self.multicorn._ldap_metadatas[connect_point] = connection
        self.ldap = connection

    def register(self, name):
        type = List(corn=self, name=name,
                    inner_type=Type(corn=self, type=unicode))
        self.properties[name] = type

    def _ldap_to_item(self, ldap_item):
        item = {}
        for name in self.properties.keys():
            item[name] = (tuple(ldap_item[name])
                          if ldap_item.get(name, None) is not None else None)
        return self.create(item)

    def _all(self):
        for _, item in self.ldap.search_s(
            self.path, ldap.SCOPE_ONELEVEL,
            "objectClass=%s" % self.objectClass,
            # Restrict results to declared properties:
            [prop.name for prop in self.properties.values()]):
            yield self._ldap_to_item(item)

    def delete(self, item):
        self.ldap.delete_s(item.dn)

    def save(self, item):
        modifications = {}
        for key in item:
            if item[key] is not None:
                modifications[key] = item[key]

        old_item = self.all.filter(c.cn == item["cn"]).one(None).execute()

        if old_item:
            self.ldap.modify_s(item.dn, ldap.modlist.modifyModlist(
                dict(old_item.itemslist()), modifications))
        else:
            modifications['objectClass'] = (self.objectClass,)
            self.ldap.add_s(item.dn, modifications.items())
