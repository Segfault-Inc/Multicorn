#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script

import test.kraken

action_runserver = werkzeug.script.make_runserver(
    test.kraken.make_site, use_reloader=True, use_debugger=True)

def action_test(packages='kalamar,koral,kraken,test', verbose=('v', False),
                coverage=('c', False), todo=('t', False)):
    """Run all doctests and unittests found in "packages"."""
    packages = packages.split(',')
    verbosity = 2 if verbose else 1
    if coverage:
        test.run_tests_with_coverage(packages, verbosity)
    else:
        test.run_tests(packages, verbosity)
    if todo:
        todos = list(test.find_TODOs(packages))
        if todos:
            width = max(len(module) for module, count, lines in todos)
            for module, count, lines in todos:
                print '%-*s' % (width, module), ':', count,
                if count > 1:
                    print 'TODOs on lines',
                else:
                    print 'TODO  on line ',
                print ', '.join(str(line) for line in lines)

def main(*args):
    werkzeug.script.run(namespace=dict(action_test=action_test,
                                       action_runserver=action_runserver),
                        args=list(args))

if __name__ == '__main__':
    main(*sys.argv[1:])
