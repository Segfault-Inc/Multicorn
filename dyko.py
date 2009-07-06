#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script

import test.kraken

action_runserver = werkzeug.script.make_runserver(
    test.kraken.make_site, use_reloader=True, use_debugger=True)

def action_test(packages='kalamar,koral,kraken,test', coverage=False, todo=False):
    """Run all doctests and unittests found in "packages"."""
    packages = packages.split(',')
    if coverage:
        test.run_tests_with_coverage(packages)
    else:
        test.run_tests(packages)
    if todo:
        for module, todos in test.find_TODOs(packages):
            print module, ':', todos, 'TO' 'DO'+('s' if todos > 1 else '')

if __name__ == '__main__':
    werkzeug.script.run()
