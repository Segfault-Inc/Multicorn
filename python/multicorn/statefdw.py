"""A dummy foreign data wrapper"""


from . import ForeignDataWrapper
from .utils import log_to_postgres
from logging import ERROR, DEBUG, INFO, WARNING


class StateFdw(ForeignDataWrapper):
    """A dummy foreign data wrapper.

    This dummy foreign data wrapper is intended as a proof of concept of state
    keeping foreign data wrappers.

    It keeps an internal state as an integer, auto-incremented at each request.
    """

    def __init__(self, *args):
        super(StateFdw, self).__init__(*args)
        self.state = 0

    def execute(self, quals):
        self.state += 1
        yield [self.state]
