from . import requests


class RequestWrapper(object):
    class_map = {}

    @classmethod
    def register_wrapper(cls, wrapped_class):
        def decorator(wrapper_class):
            cls.class_map[wrapped_class] = wrapper_class
            return wrapper_class
        return decorator
    
    @classmethod
    def from_request(cls, request):
        return cls.class_map[type(request)](request)
    
    def __init__(self, wrapped_request):
        self.wrapped_request = wrapped_request
#        self.wrapped_state = getattr(wrapped_request,
#            '_%s__state' % type(wrapped_request).__name__)
        # Directly get from __dict__ to avoid name mangling.
        self.wrapped_state = wrapped_request.__dict__['__state']
    
    def __getattr__(self, name):
        return getattr(self.wrapped_state, name)


@RequestWrapper.register_wrapper(requests.Literal)
class LiteralWrapper(RequestWrapper):
    pass


@RequestWrapper.register_wrapper(requests.Root)
class RootWrapper(RequestWrapper):
    pass


@RequestWrapper.register_wrapper(requests.Operation)
class OperationWrapper(RequestWrapper):
    pass



