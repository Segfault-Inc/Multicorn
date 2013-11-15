"""
A CSV Foreign Data Wrapper

"""


from . import ForeignDataWrapper
from .utils import log_to_postgres
from logging import INFO, WARNING, ERROR
import csv
import glob
import gzip
import itertools
import os
import re


class CsvFdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing csv files.

    Valid options:
        - filename : Full path to the csv file, which must be readable
          by the user running postgresql (usually postgres).
        - globs : Glob for recognizing files to process, which must be
          readable by the user running postgresql (usually postgres). Multiple
          globs may be passed, separated by ' // ' (space-slash-slash-space).
          (UNIX paths can not meaningfully contain two slashes.)
        - delimiter : The delimiter used between fields.
          Default: ","
        - quotechar : The CSV quote character.
          Default: '"'
        - skip_header : The number of lines to skip.
          Default: 0
        - gzip : Look for GZip file magic and decompress files if found.
          Default: true
    """

    def __init__(self, fdw_options, fdw_columns):
        super(CsvFdw, self).__init__(fdw_options, fdw_columns)
        self.filename = fdw_options.get("filename")
        self.delimiter = fdw_options.get("delimiter", ",")
        self.quotechar = fdw_options.get("quotechar", '"')
        self.skip_header = int(fdw_options.get("skip_header", 0))
        self.columns = fdw_columns
        self.globs = set(fdw_options.get("globs", "").split(" // "))
        self.globs.discard("")
        if not (bool(self.globs) ^ bool(self.filename)):
            log_to_postgres("Please set either 'filename' or 'globs'.", ERROR)
        try:
            self.gzip = {"true":  True,
                         "false": False}[fdw_options.get("gzip", "true")]
        except KeyError:
            log_to_postgres("Please set 'gzip' as 'true' or 'false'.", ERROR)


    def execute(self, quals, columns):
        generators = (self.load_file(path, columns) for path in self.paths())
        return itertools.chain(*generators)

    def load_file(self, path, columns):
        open_function = self.opener(path)
        with open_function(path) as stream:
            reader = csv.reader(stream, delimiter=self.delimiter)
            count = 0
            checked = False
            for line in reader:
                if count >= self.skip_header:
                    if not checked:
                        # On first iteration, check if the lines are of the
                        # appropriate length
                        checked = True
                        if len(line) > len(self.columns):
                            log_to_postgres("More columns than defined in "
                                            "table: %s" % path, WARNING)
                        if len(line) < len(self.columns):
                            log_to_postgres("Fewer columns than defined in "
                                            "table: %s" % path, WARNING)
                    yield line[:len(self.columns)]
                count += 1

    def opener(self, path):
        with open(path) as stream:
            if self.gzip and stream.read(2) == b'\x1F\x8b':
                log_to_postgres("Reading CSV with gzip: %s" % path, INFO)
                return gzip.open
            return open

    def paths(self):
        if self.globs:
            joined = itertools.chain(*(glob.iglob(g) for g in self.globs))
            return (p for p in joined if os.path.isfile(p))
        else:
            return [self.filename]
