# coding: utf8
import sys
import os
import doctest
import unittest2

# These packages are tested by default
DYKO_PACKAGES = ['kalamar', 'koral', 'kraken', 'test']

def run_with_coverage(function, packages):
    import coverage
    try:
        # Coverage v3 API
        c = coverage.coverage()
    except coverage.CoverageException:
        # Coverage v2 API
        c = coverage

    c.exclude('return NotImplemented')
    c.exclude('raise NotImplementedError')
    c.exclude('except ImportError:')
    c.start()
    function()
	# TODO: it seems the execution doesn’t get here.
    c.stop()
#    c.report([werkzeug.import_string(name).__file__ 
#              for name in find_all_modules(packages)])
    c.report()


def profile(function, filename):
    import cProfile
    cProfile.runctx('function()', {}, locals(), filename)

class DoctestLoader(unittest2.TestLoader):
    """
    A test loader that also loads doctests.
    """
    def loadTestsFromModule(self, module, use_load_tests=True):
#        print module.__name__,
        suite = unittest2.TestLoader.loadTestsFromModule(self, module,
                                                         use_load_tests)
        try:
            doctests = doctest.DocTestSuite(module)
        except ValueError, e:
            # doctest.DocTestSuite throws ValueError when there is no test
            if len(e.args) != 2 or e.args[1] != "has no tests":
                raise
        else:
            suite.addTest(doctests)
#        print '.py'
        return suite
        
    def _find_tests(self, start_dir, pattern):
#        print start_dir
        return unittest2.TestLoader._find_tests(self, start_dir, pattern)

def suite(packages=None):
    """Build a test suite by running the test discovery in each package."""
    project_dir = os.path.dirname(os.path.dirname(__file__))
    suite = unittest2.TestSuite()
    loader = DoctestLoader()
    for package in packages or DYKO_PACKAGES:
        suite.addTest(loader.discover(package, '*.py', project_dir))
    return suite

def main():
    unittest2.main(defaultTest='test.suite', catchbreak=True, buffer=True)

def main_coverage():
    print "Running tests with coverage."
    run_with_coverage(main, DYKO_PACKAGES)

def main_profile(filename='./profile_results'):
    print "Profiling tests."
    profile(main, filename)
    print "Profile results saved in %r. " \
          "Use the pstats module to read it." % filename

