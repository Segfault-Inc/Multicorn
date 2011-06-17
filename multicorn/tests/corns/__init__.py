from attest import Tests
from functools import wraps
from . import tests
from multicorn import Multicorn
from multicorn import colorize


def module_name_docstring(fun, module_name):
    @wraps(fun)
    def wrapper(arg):
        return fun(arg)
    wrapper.__doc__ = "%s]%s" % (
        colorize('yellow', module_name),
        colorize('green', fun.__doc__))
    return wrapper


def make_test_suite(make_corn, teardown=None):
    testinstance = Tests()

    def context():
        mc = Multicorn()
        corn = make_corn()
        mc.register(corn)
        try:
            yield corn
        finally:
            if teardown:
                teardown(corn)

    testinstance.context(context)
    for test_prototype in dir(tests):
        test_prototype = getattr(tests, test_prototype)
        if hasattr(test_prototype, '_is_corn_test'):
            testinstance.test(
                module_name_docstring(
                    test_prototype,
                    make_corn.__module__.split(".")[-1]))
    return testinstance
