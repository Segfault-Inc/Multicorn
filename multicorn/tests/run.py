#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests
from attest.hook import AssertImportHook

def run():
    with AssertImportHook():
        Tests("multicorn.tests").main()

if __name__ == '__main__':
    run()
