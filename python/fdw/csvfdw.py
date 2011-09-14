from . import ForeignDataWrapper
from datetime import date


class CsvFdw(ForeignDataWrapper):

    def __init__(self, fdw_options):
        super(CsvFdw, self).__init__(fdw_options)
        print "Csv: %r" % fdw_options
        self.filename = fdw_options["filename"]
        # self.filename = fdw_options["filename"]

    def execute(self):
        import csv
        with open(self.filename) as fd:
            reader = csv.reader(fd)
            for line in reader:
                print line
                yield line
        # with open(self.filename) as csv:
            # for line in csv.readLine():
                # yield line
