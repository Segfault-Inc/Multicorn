"""
An Amazon S3 Foreign Data Wrapper

"""
from uuid import uuid1

from . import ForeignDataWrapper
from .utils import log_to_postgres
from logging import WARNING
import csv

import boto



class S3Fdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing csv files.

    Valid options:
        - filename : full path to the csv file, which must be readable
          by the user running postgresql (usually postgres)
        - delimiter : the delimiter used between fields.
          Default: ","
    """

    def __init__(self, fdw_options, fdw_columns):
        super(S3Fdw, self).__init__(fdw_options, fdw_columns)
        self.filename = fdw_options["filename"]

        self.bucket = fdw_options['bucket']
        self.aws_access_key = fdw_options['aws_access_key']
        self.aws_secret_key = fdw_options['aws_secret_key']

        self.delimiter = fdw_options.get("delimiter", ",")
        self.quotechar = fdw_options.get("quotechar", '"')
        self.skip_header = int(fdw_options.get('skip_header', 0))
        self.columns = fdw_columns

    def execute(self, quals, columns):
        conn = boto.connect_s3(self.aws_access_key, self.aws_secret_key)
        bucket = conn.get_bucket(self.bucket)

        key = bucket.get_key(self.filename)

        # Create a tmp file path
        id = uuid1()
        file_path = '/tmp/{0}.csv'.format(id)

        # Write the bucket data to this file path
        key.get_contents_to_filename(file_path)

        with open(file_path) as stream:
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
                            log_to_postgres("There are more columns than "
                                            "defined in the table", WARNING)
                        if len(line) < len(self.columns):
                            log_to_postgres("There are less columns than "
                                            "defined in the table", WARNING)
                    yield line[:len(self.columns)]
                count += 1
