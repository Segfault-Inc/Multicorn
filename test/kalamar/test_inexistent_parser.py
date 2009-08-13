# coding: utf8

from __future__ import with_statement

import os
from unittest import TestCase
import kalamar.utils
import warnings


class TestInexistentParser(TestCase):
    
    conf = os.path.join(os.path.dirname(__file__), 'data',
                       'inexistent_parser.conf')

    def test_fail_on_inexistent_parer(self):
        self.assertRaises(kalamar.utils.ParserNotAvailable,
                          kalamar.Site, self.conf)

    def test_warn_on_inexistent_parer(self):
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Trigger a warning.
            kalamar.Site(self.conf, fail_on_inexistent_parser=False)
            # Verify some things
            self.assertEquals(len(w), 1)
            self.assert_(w[-1].category is UserWarning)
            self.assertEquals(str(w[-1].message), "The access point 'foo' was "
                              "ignored. (Unknown parser: inexistent)")

