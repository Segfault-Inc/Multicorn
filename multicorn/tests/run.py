#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
from attest import Tests
from attest.hook import AssertImportHook


def run(suffix=None):
    suffix = ".corns.%s" % suffix if suffix else ""
    with AssertImportHook():
        Tests("multicorn.tests%s" % suffix).main()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        suffix = sys.argv[1]
        run(suffix)
    run()
