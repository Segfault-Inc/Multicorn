#!/usr/bin/env python

import sys, os

sys.path.insert(0, os.path.dirname(__file__))

import werkzeug.script
import kraken
import test

action_runserver = werkzeug.script.make_runserver(
    kraken.site.Site, use_reloader=True, use_debugger=True
)
                                         
def action_test(coverage=False, print_todos=False):
    if coverage:
        test.run_tests_with_coverage()
    else:
        test.run_tests()
    if print_todos:
        for module, todos in test.find_TODOs():
                print module, ':', todos, 'TO' 'DO'+('s' if todos>1 else '')

if __name__ == '__main__':
    werkzeug.script.run()

