# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .expressions import Operation, Literal, Variable
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

    elif isinstance(expression, Operation) and expression.op_name == 'and':
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


def isolate_values(expression):
    """
    Return `(values, remainder)` such that `values` is a dict of name: value
    pairs and `(r.n1 == v1) & (r.n2 == v2) & ... & remainder` is equivalent
    to `expression`, with `values` as big as possible.
    """
    if isinstance(expression, Operation):
        if expression.op_name == 'eq':
            a, b = expression.args
            if not isinstance(a, Variable):
                # In case we have `4 == r.foo`
                b, a = a, b
            if isinstance(a, Variable) and isinstance(b, Literal):
                return {a.name: b.value}, Literal(True)
        elif expression.op_name == 'and':
            values = {}
            remainder = Literal(True)
            for arg in expression.args:
                these_values, this_remainder = isolate_values(arg)
                for key, value in these_values.iteritems():
                    if values.setdefault(key, value) != value:
                        # Two different values for the same name:
                        # r.foo == 4 & r.foo == 5 is always False.
                        return {}, Literal(False)
                remainder &= this_remainder
            return values, remainder
    return {}, expression


def isolate_values_from_query(query, isolated_variables):
    expression, query_remainder = isolate_query(query, isolated_variables)
    values, expression_remainder = isolate_values(expression)
    return values, Query.where(expression_remainder) + query_remainder

