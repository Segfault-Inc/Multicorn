""" 
@author : Grégoire ROBIN <postgres@nodashi.com>
@author : Kévin ZIEMIANSKI <kevin.ziemianski@gmail.com>
Require pymssql => http://pymssql.sourceforge.net/
"""

"""An Mssql foreign data wrapper"""
from . import ForeignDataWrapper
import pymssql
import re


class MssqlFdw(ForeignDataWrapper):
    """A Mssql foreign data wrapper.

    The  Mssql foreign data wrapper performs simple selects on a remote Mssql
    database.

    Accepted options:

    connection  --  the Mssql connection url
    tablename   --  the Mssql table name.

    """
    def addslashes(self, s):
        return re.sub("(\\\\|'|\")", lambda o: "\\" + o.group(1), s)
    
    def __init__(self, fdw_options, fdw_columns):
        super(MssqlFdw, self).__init__(fdw_options, fdw_columns)
        self.connection = pymssql.connect(host=fdw_options["host"], user=fdw_options["user"], password=fdw_options["password"], database=fdw_options["database"], as_dict=True)
        self.tablename = fdw_options["tablename"]

    def execute(self, quals, columns):
        """Execute the query against the Mssql database.

        The quals are turned into an and'ed where clause.

        """
        cursor = self.connection.cursor()
        
        """ Building where """
        have_where = False
        where = ''
        parameters = []
        for qual in quals:
            if have_where == False:
                where += ' where '
            else:
                where += ' and '
            where += ' %s %s  \'%s\'' % (
                qual.field_name, qual.operator, self.addslashes(qual.value))
            parameters.append(qual.value)
            have_where = True
            
        """ Building request """
        request = "select "
        request += ','.join(columns)
        request += " from %s %s" % (
            self.tablename, where)
            
        """ Requesting & fetching """
        cursor.execute(request)
        have_next = True
        while  have_next:
            row = cursor.fetchone()
            if row == None:
                have_next = False
            else:
                yield row

