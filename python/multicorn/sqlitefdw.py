from . import ForeignDataWrapper
from sqlite3 import connect


class SqliteFdw(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(SqliteFdw, self).__init__(fdw_options, fdw_columns)
        self.connection = connect(fdw_options["filename"])
        self.tablename = fdw_options["tablename"]

    def execute(self, quals):
        c = self.connection.cursor()
        where = ''
        parameters = []
        for qual in quals:
            where += ' and %s %s ?' % (
                qual.field_name, qual.operator)
            parameters.append(qual.value)
        request = "select * from %s where 1 %s" % (
            self.tablename, where)
        print request, " ", parameters
        c.execute(request, parameters)
        for row in c:
            yield row
