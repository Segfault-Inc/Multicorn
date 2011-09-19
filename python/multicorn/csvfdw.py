"""
A CSV Foreign Data Wrapper

"""



from . import ForeignDataWrapper
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

    def execute(self, quals):
        with open(self.filename) as stream:
            reader = csv.reader(stream, delimiter=self.delimiter)
            for line in reader:
                yield line
