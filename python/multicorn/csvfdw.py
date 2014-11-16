"""
A CSV Foreign Data Wrapper

"""


from . import ForeignDataWrapper
from .utils import log_to_postgres
from logging import WARNING
import csv


class CsvFdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing csv files.

    Valid options:
        - filename : full path to the csv file, which must be readable
          by the user running postgresql (usually postgres)
        - delimiter : the delimiter used between fields.
          Default: ","
    """

    def __init__(self, fdw_options, fdw_columns):
        super(CsvFdw, self).__init__(fdw_options, fdw_columns)
        self.filename = fdw_options["filename"]
        self.delimiter = fdw_options.get("delimiter", ",")
        self.quotechar = fdw_options.get("quotechar", '"')
        self.skip_header = int(fdw_options.get('skip_header', 0))
        self.columns = fdw_columns

    def execute(self, quals, columns):
        with open(self.filename) as stream:
            reader = csv.reader(stream, delimiter=self.delimiter, quotechar=self.quotechar)
            try:
                # Skipp the first line if it is a header
                if self.skip_header:
                    next(reader)
                # first check lenght of the first line
                line = next(reader)
                if len(line) > len(self.columns):
                    log_to_postgres("There are more columns than "
                                    "defined in the table", WARNING)
                if len(line) < len(self.columns):
                    log_to_postgres("There are less columns than "
                                    "defined in the table", WARNING)
                while True:
                    yield line[:len(self.columns)]
                    line = next(reader)
            except StopIteration:
                pass

