from . import ForeignDataWrapper
from datetime import date


class CsvFdw(ForeignDataWrapper):

    def __init__(self, fdw_options):
        super(CsvFdw, self).__init__(fdw_options)
        print "Csv: %r" % fdw_options
        # self.filename = fdw_options["filename"]

    def execute(self):
        for i in (1337, 42, 1, -23, 0.235):
            yield [i, 'line %i' % i, date.today()]
            yield {'field1': i, 'field2': 'grou%d' % i, 'field3': date.today()}
        # with open(self.filename) as csv:
            # for line in csv.readLine():
                # yield line
