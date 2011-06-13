from .requests import ContextRequest, WithRealAttributes, OperationRequest

def cut_request(request, after):
    """Cut the request in two equivalent requests such as "after" is the
    last request in the left hand side."""
    chain = as_chain(request)
    if chain[-1] is after:
        return chain, []
    for idx, request_part in enumerate(chain):
        if request_part is after:
            empty_context = ContextRequest()
            tail = WithRealAttributes(chain[-1]).copy_replace(after, empty_context)
            return after, tail
    raise ValueError("The given delimitor request is not in the request")

def as_chain(request):
    """Return a (wrapped) request as a chain of successive operations"""
    chain = [request]
    request = WithRealAttributes(request)
    if issubclass(request.obj_type(), OperationRequest):
        chain = as_chain(request.args[0]) + chain
    return chain
