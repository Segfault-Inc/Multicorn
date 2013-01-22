"""
An LDAP foreign data wrapper.

"""

from . import ForeignDataWrapper
from .utils import log_to_postgres, WARNING
import ldap


class LdapFdw(ForeignDataWrapper):
    """An Ldap Foreign Wrapper.

    The following options are required:

    uri		-- the ldap URI to connect. (ex: 'ldap://localhost')
    address     -- the ldap host to connect. (obsolete)
    path        -- the ldap path (ex: ou=People,dc=example,dc=com)
    objectClass -- the ldap object class (ex: 'inetOrgPerson')
    scope	-- the ldap scope (one, sub or base)

    """

    def __init__(self, fdw_options, fdw_columns):
        super(LdapFdw, self).__init__(fdw_options, fdw_columns)
	if fdw_options["address"] == None:
	    self.ldapuri = fdw_options["uri"]
	else:
	    self.ldapuri = "ldap://" + fdw_options["address"]
        self.ldap = ldap.initialize(self.ldapuri)
        self.path = fdw_options["path"]
	self.scope = self.parse_scope(fdw_options["scope"])
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
            self.path, self.scope, request):
            yield [
               item.get(field, [None])[0]
               for field in self.field_list]

    def parse_scope(self,scope):
	if scope == None:
	    return ldap.SCOPE_ONELEVEL
	elif scope == "":
	    return ldap.SCOPE_ONELEVEL
	elif scope == "one":
	    return ldap.SCOPE_ONELEVEL
	elif scope == "sub":
	    return ldap.SCOPE_SUBTREE
	elif scope == "base":
	    return ldap.SCOPE_BASE
	else:
	    log_to_postgres("Invalid scope specified: %s" % scope,ERROR)	
