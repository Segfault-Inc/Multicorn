from . import ForeignDataWrapper
import csv


class CsvFdw(ForeignDataWrapper):

    def __init__(self, fdw_options):
        super(CsvFdw, self).__init__(fdw_options)
        print "Csv: %r" % fdw_options
        self.filename = fdw_options["filename"]
        # self.filename = fdw_options["filename"]

    def execute(self):
        with open(self.filename) as fd:
            reader = csv.reader(fd)
            for line in reader:
                yield line
