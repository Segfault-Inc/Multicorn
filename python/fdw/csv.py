from . import ForeignDataWrapper


class CsvFdw(ForeignDataWrapper):

    def __init__(self, fdw_options):
        super(CsvFdw, self).__init__(fdw_options)
        print "Csv"
        # self.filename = fdw_options["filename"]

    def execute(self):
        return ["1337", "42"]
        # with open(self.filename) as csv:
            # for line in csv.readLine():
                # yield line
