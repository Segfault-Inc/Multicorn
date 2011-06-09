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
        for class_ in type(request).mro():
            wrapper_class = cls.class_map.get(class_, None)
            if wrapped_class is not None:
                return wrapper_class(request)
        raise TypeError('No request wrapper for type %s.' % type(request))
    
    def __init__(self, wrapped_request):
        self.wrapped_request = wrapped_request
        self.args = wrapped_request._Request__args


@RequestWrapper.register_wrapper(requests.Literal)
class LiteralWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(LiteralWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args


@RequestWrapper.register_wrapper(requests.List)
class ListWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(ListWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = [self.from_request(r) for r in self.value]


@RequestWrapper.register_wrapper(requests.Tuple)
class TupleWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(TupleWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = tuple(self.from_request(r) for r in self.value)


@RequestWrapper.register_wrapper(requests.Dict)
class DictWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(DictWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = dict(
            (key, self.from_request(value)) 
            for key, value in self.value.iteritems())


@RequestWrapper.register_wrapper(requests.Root)
class RootWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(RootWrapper, self).__init__(*args, **kwargs)
        self.scope_depth, = self.args


@RequestWrapper.register_wrapper(requests.Operation)
class OperationWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(OperationWrapper, self).__init__(*args, **kwargs)
        self.args = tuple(self.from_request(r) for r in self.args)


