from operator import __and__
from .expressions import Operation, Literal
from . import _Query, Query


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


def isolate_query(query, isolated_variables):
    """
    Return `(e, q)` such that `Query.where(e) + q` is equivalent to `query`,
    and `e` is only about variables listed in `isolated_variables`.
    
    Worst cases will return `(Literal(True), query)`.
    """
    if not query.operations or query.operations[0].kind != 'where':
        return (Literal(True), query)
    
    # Remove the where from the query
    condition, = query.operations[0].args
    query = _Query(query.operations[1:])
    
    e1, e2 = isolate_expression(condition, isolated_variables)
    # Recurse in case there are more than one where operation.
    e3, query = isolate_query(query, isolated_variables)
    
    return (e1 & e3, Query.where(e2) + query)
