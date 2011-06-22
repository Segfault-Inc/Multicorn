from .requests import LiteralRequest, ContextRequest, as_chain, WithRealAttributes
from .wrappers import RequestWrapper, EqWrapper, BinaryOperationWrapper, AndWrapper, LiteralWrapper, MulWrapper
from . import wrappers

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

def collect(request, predicate):
    matches = []
    def visitor(chain_item):
        if predicate(chain_item):
            matches.append(chain_item)
    object.__getattribute__(request, '_visit')(visitor)
    return matches

def cut_on_predicate(request, predicate, recursive=False, position=-1):
    """Cut a request as soon as a request matching the predicate is found.
    When a request_part matches the predicate, the chain is cut at the relative position
    `position`.
    [:index(matching_part) + position], [index(matching_part) + position:]
    If no chain item matches the predicate,  (None, request) is returned.
    Else, (before, matching_request +rest) is returned.
    """
    if recursive:
        def matcher(req):
            return collect(req, predicate)
    else:
        matcher = predicate
    chain = as_chain(request)
    for idx, request_part in enumerate(chain):
        if matcher(request_part):
            empty_context = ContextRequest()
            tail = WithRealAttributes(chain[-1])._copy_replace({chain[idx + position]:
                empty_context})
            return chain[idx + position], tail
    return None, request

def cut_on_index(request, index):
    chain = as_chain(request)
    empty_context = ContextRequest()
    tail = WithRealAttributes(chain[-1])._copy_replace(chain[index],
                empty_context)
    return chain[index], tail



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
            if isinstance(a, LiteralWrapper):
                # In case we have `4 == r.foo`
                b, a = a, b
                typeb, typea = typea, typeb
            elif not isinstance(b, LiteralWrapper):
                # Neither is a Literal, we should GTFO
                return {}, expression
            # If b is equal to newb, then we do not have any other
            # variables and the value is assumed to be a literal
            newb, remainder = inner_split(b, [], contexts)
            value = (newb.wrapped_request.execute())
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

def inject_context(request, context_values=()):
    replacements = {}
    def find_matching(context):
        def func(request):
            return WithRealAttributes(request).subject == context
        return func
    def visitor(req):
        chain = as_chain(req)
        if isinstance(chain[0], ContextRequest):
            wrareq = WithRealAttributes(req)
            wracontext = WithRealAttributes(chain[0])
            if wracontext.scope_depth < 0 and req is not chain[0]:
                newreq = wrareq._copy_replace({chain[0]: LiteralRequest(context_values[wracontext.scope_depth])})
                newvalue = LiteralRequest(newreq.execute())
                replacements[req] = newvalue
    request._visit(visitor)
    return request._copy_replace(replacements)


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
