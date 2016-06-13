"""
Purpose
-------

This fdw can be used to access data stored in `CSV files`_. Each column defined
in the table will be mapped, in order, against columns in the CSV file.

.. api_compat:: :read:

.. _CSV files: http://en.wikipedia.org/wiki/Comma-separated_values

Dependencies
------------

No dependency outside the standard python distribution.

Options
----------------

``filename`` (required)
  The full path to the CSV file containing the data. This file must be readable
  to the postgres user.

``delimiter``
  The CSV delimiter (defaults to  ``,``).

``quotechar``
  The CSV quote character (defaults to ``"``).

``skip_header``
  The number of lines to skip (defaults to ``0``).

Usage example
-------------

Supposing you want to parse the following CSV file, located in ``/tmp/test.csv``::

    Year,Make,Model,Length
    1997,Ford,E350,2.34
    2000,Mercury,Cougar,2.38

You can declare the following table:

.. code-block:: sql

    CREATE SERVER csv_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.csvfdw.CsvFdw'
    );


    create foreign table csvtest (
           year numeric,
           make character varying,
           model character varying,
           length numeric
    ) server csv_srv options (
           filename '/tmp/test.csv',
           skip_header '1',
           delimiter ',');

    select * from csvtest;

.. code-block:: bash

     year |  make   | model  | length
    ------+---------+--------+--------
     1997 | Ford    | E350   |   2.34
     2000 | Mercury | Cougar |   2.38
    (2 lines)


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
