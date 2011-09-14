from . import ForeignDataWrapper
import ldap


class LdapFdw(ForeignDataWrapper):

    def __init__(self, fdw_options):
        super(LdapFdw, self).__init__(fdw_options)
        print fdw_options
        self.ldap = ldap.open(fdw_options["address"])
        self.path = fdw_options["path"]
        self.objectClass = fdw_options["objectclass"]

    def execute(self):
        for _, item in self.ldap.search_s(
            self.path, ldap.SCOPE_ONELEVEL,
            "(objectClass=%s)" % self.objectClass):
            yield item["cn"][0], item["sn"][0], item["givenName"][0]
