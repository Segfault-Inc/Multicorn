#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script

def run_tests(packages='kalamar,koral,kraken,test', verbose=('v', False),
                coverage=('c', False), todo=('t', False)):
    """Run all doctests and unittests found in "packages"."""
    import test.kraken
    packages = packages.split(',')
    verbosity = 2 if verbose else 1
    if coverage:
        test.run_tests_with_coverage(packages, verbosity)
    else:
        test.run_tests(packages, verbosity)
    if todo:
        test.print_TODOs(packages)

def main(*args):
    werkzeug.script.run(namespace={'test': run_tests}, action_prefix='',
                        args=['test'] + list(args))

if __name__ == '__main__':
    main(*sys.argv[1:])
