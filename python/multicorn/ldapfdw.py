"""
An LDAP foreign data wrapper.

"""

from . import ForeignDataWrapper

import ldap3
from multicorn.utils import log_to_postgres, ERROR
from multicorn.compat import unicode_


SPECIAL_CHARS = {
    ord('*'): '\\2a',
    ord('('): '\\28',
    ord(')'): '\29',
    ord('\\'): '\\5c',
    ord('\x00'): '\\00',
    ord('/'): '\\2f'
}


class LdapFdw(ForeignDataWrapper):
    """An Ldap Foreign Wrapper.

    The following options are required:

    uri         -- the ldap URI to connect. (ex: 'ldap://localhost')
    address     -- the ldap host to connect. (obsolete)
    path        -- the ldap path (ex: ou=People,dc=example,dc=com)
    objectClass -- the ldap object class (ex: 'inetOrgPerson')
    scope       -- the ldap scope (one, sub or base)
    binddn      -- the ldap bind DN (ex: 'cn=Admin,dc=example,dc=com')
    bindpwd     -- the ldap bind Password

    """

    def __init__(self, fdw_options, fdw_columns):
        super(LdapFdw, self).__init__(fdw_options, fdw_columns)
        if "address" in fdw_options:
            self.ldapuri = "ldap://" + fdw_options["address"]
        else:
            self.ldapuri = fdw_options["uri"]
        self.ldap = ldap3.Connection(
            ldap3.Server(self.ldapuri),
            user=fdw_options.get("binddn", None),
            password=fdw_options.get("bindpwd", None),
            client_strategy=ldap3.STRATEGY_SYNC_RESTARTABLE)
        self.path = fdw_options["path"]
        self.scope = self.parse_scope(fdw_options.get("scope", None))
        self.object_class = fdw_options["objectclass"]
        self.field_list = fdw_columns
        self.field_definitions = dict(
            (name.lower(), field) for name, field in self.field_list.items())
        self.array_columns = [
            col.column_name for name, col in self.field_definitions.items()
            if col.type_name.endswith('[]')]

    def execute(self, quals, columns):
        request = unicode_("(objectClass=%s)") % self.object_class
        for qual in quals:
            if isinstance(qual.operator, tuple):
                operator = qual.operator[0]
            else:
                operator = qual.operator
            if operator in ("=", "~~"):
                if hasattr(qual.value, "translate"):
                    baseval = qual.value.translate(SPECIAL_CHARS)
                    val = (baseval.replace("%", "*")
                           if operator == "~~" else baseval)
                else:
                    val = qual.value
                request = unicode_("(&%s(%s=%s))") % (
                    request, qual.field_name, val)
        self.ldap.search(
            self.path, request, self.scope,
            attributes=list(self.field_definitions))
        for entry in self.ldap.response:
            # Case insensitive lookup for the attributes
            litem = dict()
            for key, value in entry["attributes"].items():
                if key.lower() in self.field_definitions:
                    pgcolname = self.field_definitions[key.lower()].column_name
                    if pgcolname in self.array_columns:
                        value = value
                    else:
                        value = value[0]
                    litem[pgcolname] = value
            yield litem

    def parse_scope(self, scope=None):
        if scope in (None, "", "one"):
            return ldap3.SEARCH_SCOPE_SINGLE_LEVEL
        elif scope == "sub":
            return ldap3.SEARCH_SCOPE_WHOLE_SUBTREE
        elif scope == "base":
            return ldap3.SEARCH_SCOPE_BASE_OBJECT
        else:
            log_to_postgres("Invalid scope specified: %s" % scope, ERROR)
