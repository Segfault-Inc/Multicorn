from . import ForeignDataWrapper

class StateFdw(ForeignDataWrapper):

    def __init__(self, *args):
        super(StateFdw, self).__init__(*args)
        self.state = 0

    def execute(self, quals):
        self.state += 1
        yield [self.state]

