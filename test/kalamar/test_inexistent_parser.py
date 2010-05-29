# coding: utf8

import os
from unittest2 import TestCase
import kalamar.utils
import warnings



class TestInexistentParser(TestCase):
    conf = os.path.join(os.path.dirname(__file__), 'data',
                       'inexistent_parser.conf')

    def test_fail_on_inexistent_parser(self):
        self.assertRaises(kalamar.utils.ParserNotAvailable,
                          kalamar.Site, self.conf)

    def test_warn_on_inexistent_parser(self):
        # Save warnings options
        # Quite the same as warnings.catch_warnings but python<2.6 compatible
        log = []
        old_showwarning = warnings.showwarning
        old_filters = warnings.filters
        warnings.showwarning = \
            lambda message, category, filename, lineno, file=None, line=None: \
            log.append({
                'message': message, 'category': category, 'filename': filename,
                'lineno': lineno, 'file': file, 'line': line})

        # Cause all warnings to always be triggered.
        warnings.simplefilter("always")
        # Trigger a warning
        kalamar.Site(self.conf, fail_on_inexistent_parser=False)
        # Verify some things
        self.assertEquals(len(log), 1)
        self.assert_(log[-1]["category"] is UserWarning)
        self.assertEquals(str(log[-1]["message"]), "The access point 'foo' was "
                          "ignored. (Unknown parser: inexistent)")

        # Stop catching warnings
        warnings.filter = old_filters
        warnings.showwarning = old_showwarning
