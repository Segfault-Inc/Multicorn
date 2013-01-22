"""
An LDAP foreign data wrapper.

"""

from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR, WARNING
import ldap


class LdapFdw(ForeignDataWrapper):
    """An Ldap Foreign Wrapper.

    The following options are required:

    uri		-- the ldap URI to connect. (ex: 'ldap://localhost')
    address     -- the ldap host to connect. (obsolete)
    path        -- the ldap path (ex: ou=People,dc=example,dc=com)
    objectClass -- the ldap object class (ex: 'inetOrgPerson')
    scope	-- the ldap scope (one, sub or base)
    binddn	-- the ldap bind DN (ex: 'cn=Admin,dc=example,dc=com')
    bindpwd	-- the ldap bind Password

    """

    def __init__(self, fdw_options, fdw_columns):
        super(LdapFdw, self).__init__(fdw_options, fdw_columns)
	if "address" in fdw_options:
	    self.ldapuri = "ldap://" + fdw_options["address"]
	else:
	    self.ldapuri = fdw_options["uri"]
        self.ldap = ldap.initialize(self.ldapuri)
        self.path = fdw_options["path"]
	if "scope" in fdw_options:
	    self.scope = self.parse_scope(fdw_options["scope"])
	else:
	    self.scope = self.parse_scope()
        self.object_class = fdw_options["objectclass"]
        self.field_list = fdw_columns
	if "binddn" in fdw_options:
	    self.binddn = fdw_options["binddn"]
	else:
	    self.binddn = None
        if "bindpwd" in fdw_options:
            self.bindpwd = fdw_options["bindpwd"]
	else:
	    self.binddn = None
	self.bind()

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

    def bind(self):
        try:
    	    if self.binddn != None:
    	        if self.bindpwd != None:
    	            self.ldap.simple_bind_s(who=self.binddn,cred=self.bindpwd)
                else:	
    	            self.ldap.simple_bind_s(who=self.binddn)
	except ldap.INVALID_CREDENTIALS, msg:
	    log_to_postgres("LDAP BIND Error: %s" % msg,ERROR)	
	except ldap.UNWILLING_TO_PERFORM, msg:
	    log_to_postgres("LDAP BIND Error: %s" % msg,ERROR)	

    def parse_scope(self,scope = None):
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
