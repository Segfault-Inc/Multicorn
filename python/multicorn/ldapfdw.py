from . import ForeignDataWrapper
import ldap


class LdapFdw(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(LdapFdw, self).__init__(fdw_options)
        self.ldap = ldap.open(fdw_options["address"])
        self.path = fdw_options["path"]
        self.objectClass = fdw_options["objectclass"]
        self.field_list = fdw_columns

    def execute(self, quals):
        request = "(objectClass=%s)" % self.objectClass
        print quals
        for qual in quals:
            if qual.operator in ("=", "~~"):
                val = (qual.value.replace("%", "*")
                       if qual.operator == "~~" else qual.value)
                request = "(&%s(%s=%s))" % (
                    request, qual.field_name, val)
        print request
        for _, item in self.ldap.search_s(
            self.path, ldap.SCOPE_ONELEVEL, request):
            yield [
               item.get(field, [None])[0]
               for field in self.field_list]
