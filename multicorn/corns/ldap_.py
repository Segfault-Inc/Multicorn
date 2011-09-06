from __future__ import print_function
from multicorn.utils import colorize
from multicorn.requests import CONTEXT as c
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


class Ldap(EasyCorn):
    def __init__(self, name, hostname, path, user=None,
                 password=None, encoding="utf-8", identity_properties=("cn",)):
        super(Ldap, self).__init__(name, identity_properties)
        self.register("cn")
        self.encoding = encoding
        self.hostname = hostname
        self.path = path
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

    def _ldap_to_item(self, ldap_item):
        item = {}
        for name in self.properties.keys():
            item[name] = ldap_item.get(name, None)
        return self.create(item)

    def _all(self):
        for _, item in self.ldap.search_s(
            self.path, ldap.SCOPE_ONELEVEL, "objectClass=*",
            # Restrict results to declared properties:
            [prop.name for prop in self.properties.values()]):
            yield self._ldap_to_item(item)

    def delete(self, item):
        dn = "cn=%s,%s" % (item['cn'], self.path)
        self.ldap.delete_s(dn)

    def save(self, item):
        modifications = {}
        dn = "cn=%s,%s" % (item['cn'], self.path)
        for key in item:
            if item[key] is not None:
                    modifications[key] = item[key]

        old_item = self.all.filter(c.cn == item["cn"]).one(None).execute()

        if old_item:
            # Here we replace properties names in order to make the diff
            old_rdn_entry = {}

            self.ldap.modify_s(dn, ldap.modlist.modifyModlist(
                old_rdn_entry, modifications))
        else:
            self.ldap.add_s(dn, modifications.items())
