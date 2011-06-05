from operator import __and__
from .expressions import Operation, Literal


def isolate_expression(expression, isolated_variables):
    """
    Return `(e1, e2)` such that `e1 & e2` is equivalent to `expression` and
    `e1` is only about variables listed in `isolated_variables`.
    
    Worst cases will return `(Literal(True), query)`.
    """
    if not (set(expression.affected_variables()) - set(isolated_variables)):
        # `expression` only affects the variables we want to isolate.
        return (expression, Literal(True))

    elif isinstance(expression, Operation) and expression.name == 'and':
        a, b = expression.args
        a1, a2 = isolate_expression(a, isolated_variables)
        b1, b2 = isolate_expression(b, isolated_variables)
        return a1 & b1, a2 & b2
    
    else:
        # Could not simplify further
        return (Literal(True), expression)


