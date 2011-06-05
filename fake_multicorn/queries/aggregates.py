from .expressions import Expression, Literal


def _ensure_expression(expression):
    """
    Ensure that the given parameter is an Expression. If it is not, wrap it
    in a Literal.
    """
    if isinstance(expression, Expression):
        return expression
    else:
        return Literal(expression)


class By(object):
    def __init__(self, **criteria):
        self.criteria = criteria


class AggregateFunction(object):
    'Abstract'
    def __init__(self, expression):
        self.expression = _ensure_expression(expression)

class Sum(AggregateFunction): pass
class Min(AggregateFunction): pass
class Max(AggregateFunction): pass
class Count(AggregateFunction): pass
class Avg(AggregateFunction): pass

