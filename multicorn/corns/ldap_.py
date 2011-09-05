from __future__ import print_function
from multicorn.utils import colorize
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
    def __init__(self, name, hostname, ldap_path, user=None,
                 password=None, encoding="utf-8", identity_properties=("cn",)):
        super(Ldap, self).__init__(name, identity_properties)
        self.register("cn")
        self.encoding = encoding
        self.hostname = hostname
        self.ldap_path = ldap_path
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
            self.ldap_path, ldap.SCOPE_ONELEVEL, "objectClass=*",
            # Restrict results to declared properties:
            [prop.name for prop in self.properties.values()]):
            yield self._ldap_to_item(item)
