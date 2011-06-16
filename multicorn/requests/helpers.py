from .requests import LiteralRequest, ContextRequest
from .wrappers import RequestWrapper, EqWrapper, BinaryOperationWrapper, AndWrapper, LiteralWrapper
from ..python_executor import execute

def split_predicate(filter, types, contexts=()):
    """Takes a FilterWrapper object, and returns two predicates wrappers objects"""
    contexts = contexts + (filter.subject.return_type(contexts).inner_type,)
    return inner_split(filter.predicate, types, contexts)

def inner_split(wrapped_request, types, contexts):
    named_types = [type for type in wrapped_request.used_types(contexts).keys() if type.name]
    if not set(named_types) - set(types):
        # `expression` already only concern the desired types
        return (wrapped_request, RequestWrapper.from_request(LiteralRequest(True)))
    elif isinstance(wrapped_request, AndWrapper):
        subject1, subject2 = inner_split(wrapped_request.subject, types, contexts)
        other1, other2 = inner_split(wrapped_request.other, types, contexts)
        self_filter = RequestWrapper.from_request(subject1.wrapped_request
                & other1.wrapped_request)
        other_filter = RequestWrapper.from_request(subject2.wrapped_request
                & other2.wrapped_request)
        return  self_filter, other_filter
    else:
        return (RequestWrapper.from_request(LiteralRequest(True)), wrapped_request)


def isolate_values(expression, contexts=()):
    """
    Return `(values, remainder)` such that `values` is a dict of name: value
    pairs and `(r.n1 == v1) & (r.n2 == v2) & ... & remainder` is equivalent
    to `expression`, with `values` as big as possible.
    """
    expression = RequestWrapper.from_request(expression)
    if isinstance(expression, BinaryOperationWrapper):
        if isinstance(expression, EqWrapper):
            a, b = expression.subject, expression.other
            typea, typeb = a.return_type(contexts), b.return_type(contexts)
            if not typea.name:
                # In case we have `4 == r.foo`
                b, a = a, b
                typeb, typea = typea, typeb
            # If b is equal to newb, then we do not have any other
            # variables and the value is assumed to be a literal
            newb, remainder = inner_split(b, [], contexts)
            value = execute(newb.wrapped_request)
            return {typea.name: value}, LiteralRequest(True)
        elif isinstance(expression, AndWrapper):
            values = {}
            remainder = LiteralRequest(True)
            for this in expression.subject, expression.other:
                subject_value, subject_remainder = isolate_values(this.wrapped_request, contexts)
                for key, value in subject_value.iteritems():
                    if values.setdefault(key, value) != value:
                        # Two different values for the same name:
                        # r.foo == 4 & r.foo == 5 is always False.
                        return {}, LiteralRequest(False)
                remainder &= subject_remainder
            return values, remainder
    return {}, expression.wrapped_request

def isolate_identity_values(filter, id_types, contexts=()):
    """Return values, filter such that filter contains everything
    not concerned by the id_types in eq equality and values is a dict
    of filtered values
    """
    wrapped_filter = RequestWrapper.from_request(filter)
    contexts = contexts  + (wrapped_filter.subject.return_type().inner_type,)
    self_filter, other_filter = inner_split(wrapped_filter.predicate, id_types, contexts)
    # Isolate the values defined in a "Eq" comparison
    # Remainder is a query containing nor "Eq" comparison used
    # in the filter
    if not isinstance(self_filter, LiteralWrapper):
        values, remainder = isolate_values(self_filter.wrapped_request, contexts)
        remainder_query = ContextRequest().filter(remainder).filter(
            other_filter.wrapped_request)
        return values, remainder_query
    else:
        return {}, filter





