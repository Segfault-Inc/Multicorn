#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import werkzeug.script
import functools

def run_tests(packages='kalamar,koral,kraken,test', verbose=('v', False),
              coverage=('c', False), profile=('p', False), todo=('t', False)):
    """Run all doctests and unittests found in "packages"."""
    import test.kraken
    packages = packages.split(',')
    verbosity = 2 if verbose else 1
    run = functools.partial(test.run_tests, packages, verbosity)
    if profile:
        run = functools.partial(test.profile, run, 'profile_results')
    if coverage:
        run = functools.partial(test.run_with_coverage, run, packages)
    run()
    if todo:
        test.print_TODOs(packages)

def main(*args):
    werkzeug.script.run(namespace={'test': run_tests}, action_prefix='',
                        args=['test'] + list(args))

if __name__ == '__main__':
    main(*sys.argv[1:])
