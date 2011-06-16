#!/usr/bin/env python
from attest import Tests
from attest.hook import AssertImportHook

with AssertImportHook():
    Tests("multicorn.tests").main()
