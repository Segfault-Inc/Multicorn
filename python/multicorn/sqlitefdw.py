"""An SQLite 3 foreign data wrapper"""

from . import ForeignDataWrapper
from sqlite3 import connect


class SqliteFdw(ForeignDataWrapper):
    """A Sqlite3 foreign data wrapper.

    The sqlite foreign data wrapper performs simple selects on a remote sqlite3
    database.

    Accepted options:

    connection  --  the sqlite3 connection url
    tablename   --  the sqlite3 table name.

    """

    def __init__(self, fdw_options, fdw_columns):
        super(SqliteFdw, self).__init__(fdw_options, fdw_columns)
        self.connection = connect(fdw_options["filename"])
        self.tablename = fdw_options["tablename"]

    def execute(self, quals):
        """Execute the query against the sqlite3 database.

        The quals are turned into an and'ed where clause.

        """
        cursor = self.connection.cursor()
        where = ''
        parameters = []
        for qual in quals:
            where += ' and %s %s ?' % (
                qual.field_name, qual.operator)
            parameters.append(qual.value)
        request = "select * from %s where 1 %s" % (
            self.tablename, where)
        cursor.execute(request, parameters)
        for row in cursor:
            yield row
