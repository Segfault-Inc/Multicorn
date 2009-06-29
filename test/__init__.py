import doctest
import unittest

import werkzeug

def find_all_modules():
    for package in ('test', 'kalamar',):
        for module in werkzeug.find_modules(package, include_packages=True,
                                            recursive=True):
            yield module

def get_tests():
    """
    Return a TestSuite
    """
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    for module_name in find_all_modules():
        suite.addTests(loader.loadTestsFromName(module_name))
        try:
            tests = doctest.DocTestSuite(module_name)
        except ValueError, e:
            # doctest.DocTestSuite throws ValueError when there is no test
            if len(e.args) != 2 or e.args[1] != "has no tests":
                raise
        else:
            suite.addTests(tests)
    return suite

def find_TODOs():
    for module_name in find_all_modules():
        filename = werkzeug.import_string(module_name).__file__
        f = open(filename)
        # Write 'TO' 'DO' to prevent this script from finding itself
        todo = f.read().count('TO' 'DO')
        f.close()
        if todo:
            yield filename, todo

def run_tests():
    unittest.TextTestRunner().run(get_tests())

def run_tests_with_coverage():
    import coverage
    c = coverage.coverage()
    c.start()
    run_tests()
    c.stop()
    c.report([werkzeug.import_string(name).__file__ 
              for name in find_all_modules()])


