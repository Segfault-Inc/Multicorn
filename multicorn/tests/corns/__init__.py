from attest import Tests

from . import tests



def make_test_suite(make_corn):
    testinstance = Tests()
    def context():
        yield make_corn()
    testinstance.context(context)
    for test_prototype in dir(tests):
        test_prototype = getattr(tests, test_prototype)
        if hasattr(test_prototype, '_is_corn_test'):
            testinstance.test(test_prototype)
    return testinstance
