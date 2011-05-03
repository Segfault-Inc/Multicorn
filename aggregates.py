
class By(object):
    def __init__(self, **criteria):
        self.criteria = criteria


class AggregateFunction(object):
    'Abstract'
    def __init__(self, expression):
        self.expression = expression

class Sum(AggregateFunction): pass
class Min(AggregateFunction): pass
class Max(AggregateFunction): pass
class Count(AggregateFunction): pass
class Avg(AggregateFunction): pass

