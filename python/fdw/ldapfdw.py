from . import ForeignDataWrapper
import ldap


class LdapFdw(ForeignDataWrapper):

    def __init__(self, fdw_options):
        super(LdapFdw, self).__init__(fdw_options)
        print fdw_options
        self.address = fdw_options["address"]

    def execute(self):
        return ["cn", "sn", "givenName"]
