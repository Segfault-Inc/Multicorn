from attest import Tests

from . import tests


def make_test_suite(make_corn, teardown=None):
    testinstance = Tests()

    def context():
        corn = make_corn()
        try:
            yield corn
        finally:
            if teardown:
                teardown(corn)

    testinstance.context(context)
    for test_prototype in dir(tests):
        test_prototype = getattr(tests, test_prototype)
        if hasattr(test_prototype, '_is_corn_test'):
            testinstance.test(test_prototype)
    return testinstance
