# coding: utf8
import sys
import os
import doctest
import unittest
import unittest2


PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))

# These packages are tested by default
DYKO_PACKAGES = ['kalamar', 'koral', 'kraken', 'test']


def monkeypatch_coverage():
    """
    coverage can blacklist some filename prefixes, but we want to whitelist
    our project directory (and exclude everything else).
    Monkey-patch an internal function in coverage to do that.
    """
    
    from coverage.report import Reporter
    
    if hasattr(Reporter, '__original_find_code_units'):
        # Safeguard so that this runs only once.
        # Monkey-patching multiple times won’t break anything but will do the
        # filtering multiple times uselessly
        return
    
    Reporter.__original_find_code_units = Reporter.find_code_units
    
    def patched_find_code_units(self, *args, **kwargs):
        self.__original_find_code_units(*args, **kwargs)
        self.code_units = [
            cu for cu in self.code_units
            if cu.filename.startswith(PROJECT_DIR) and not
               # exclude buildout-installed dependencies
               cu.filename.startswith(os.path.join(PROJECT_DIR, 'eggs'))
        ]
    
    Reporter.find_code_units = patched_find_code_units

def run_with_coverage(run_function):
    monkeypatch_coverage()
    import coverage
    c = coverage.coverage()
    c.exclude('return NotImplemented')
    c.exclude('raise NotImplementedError')
    c.exclude('except ImportError:')
    c.start()
    run_function()
    c.stop()
    c.report()


def profile(function, filename):
    import cProfile
    cProfile.runctx('function()', {}, locals(), filename)

class DoctestLoader(unittest2.TestLoader):
    """
    A test loader that also loads doctests.
    """
    def loadTestsFromModule(self, module, use_load_tests=True):
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
        return suite

def make_suite(names=None):
    """Build a test suite each from each package, module, 
    test case class or method name."""
    suite = unittest2.TestSuite()
    loader = DoctestLoader()
    for name in names or sys.argv[1:] or DYKO_PACKAGES:
        # Try unittest2’s discovery
        try:
            suite.addTest(loader.discover(name, '*.py', PROJECT_DIR))
        except ImportError:
            # name could be a class or a method
            suite.addTest(loader.loadTestsFromName(name))
    return suite


def run_suite(suite):
    """
    Run a test suite with output buferring and ctrl-C catching
    """
    # Control-C during the test run waits for the current test to end and then
    # Same as --catch :
    # reports all the results so far. A second control-C raises the normal
    # KeyboardInterrupt  exception.
    unittest2.installHandler()
    
    unittest2.TextTestRunner(buffer=True).run(suite)
    
def main():
    run_suite(make_suite())

def main_coverage():
    print "Running tests with coverage."
    run_with_coverage(main)

def main_profile(filename='./profile_results'):
    print "Profiling tests."
    profile(main, filename)
    print "Profile results saved in %r. " \
          "Use the pstats module to read it." % filename

