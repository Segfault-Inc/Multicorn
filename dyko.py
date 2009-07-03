#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script

import kraken
import test

action_runserver = werkzeug.script.make_runserver(
    kraken.site.Site, use_reloader=True, use_debugger=True)
                                         
def action_test(packages='kalamar, koral, test', coverage=False, todo=False):
    """Run all doctests and unittests found in "packages"."""
    packages = packages.replace(' ', '').split(',')
    if coverage:
        test.run_tests_with_coverage(packages)
    else:
        test.run_tests(packages)
    if todo:
        for module, todos in test.find_TODOs(packages):
            print module, ':', todos, 'TO' 'DO' + ('s' * (todos>1))

if __name__ == '__main__':
    werkzeug.script.run()
