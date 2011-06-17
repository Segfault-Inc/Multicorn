#!/usr/bin/env python
from attest import Tests
from attest.hook import AssertImportHook

def run():
    with AssertImportHook():
        Tests("multicorn.tests").main()

if __name__ == '__main__':
    run()
