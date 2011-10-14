"""
An LDAP foreign data wrapper.

"""

from . import ForeignDataWrapper
from .utils import log_to_postgres, WARNING
import ldap


class LdapFdw(ForeignDataWrapper):
    """An Ldap Foreign Wrapper.

    The following options are required:

    address     -- the ldap host to connect.
    path        -- the ldap path (ex: ou=People,dc=example,dc=com)
    objectClass -- the ldap object class (ex: 'inetOrgPerson')

    """

    def __init__(self, fdw_options, fdw_columns):
        super(LdapFdw, self).__init__(fdw_options, fdw_columns)
        self.ldap = ldap.open(fdw_options["address"])
        self.path = fdw_options["path"]
        self.object_class = fdw_options["objectclass"]
        self.field_list = fdw_columns

    def execute(self, quals, columns):
        request = "(objectClass=%s)" % self.object_class
        for qual in quals:
            if qual.operator in ("=", "~~"):
                val = (qual.value.replace("%", "*")
                       if qual.operator == "~~" else qual.value)
                request = "(&%s(%s=%s))" % (
                    request, qual.field_name, val)
        for _, item in self.ldap.search_s(
            self.path, ldap.SCOPE_ONELEVEL, request):
            yield [
               item.get(field, [None])[0]
               for field in self.field_list]
