# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import sys
import functools


def as_request(obj):
    """
    Return a Request object for `obj`.
    """
    if isinstance(obj, Request):
        return obj
    elif isinstance(obj, list):
        return ListRequest(obj)
    elif isinstance(obj, tuple):
        return TupleRequest(obj)
    elif isinstance(obj, dict):
        return DictRequest(obj)
    else:
        return LiteralRequest(obj)


class WithRealAttributes(object):
    """
    Wrap a Request object to allow access to its attributes without going
    through `Request.__getattribute__` and `Request.__setattr__`.
    """
    __slots__ = ('_wrapped_obj',)

    def __init__(self, obj):
        object.__setattr__(self, '_wrapped_obj', obj)

    def __getattr__(self, name):
        return object.__getattribute__(self._wrapped_obj, name)

    def __setattr__(self, name, value):
        object.__setattr__(self._wrapped_obj, name, value)

    def obj_type(self):
        return type(self._wrapped_obj)



def self_with_attrs(method):
    """
    Decorate a method so that the first argument `self` is wrapped with
    WithRealAttributes.
    """
    @functools.wraps(method)
    def decorated_method(self, *args, **kwargs):
        return method(WithRealAttributes(self), *args, **kwargs)
    return decorated_method


class Request(object):
    """
    Abstract base class for all request objects.

    This class defines special methods like `__add__` so that python operators
    can be used on requests, as in `req = req1 + req2`.

    In particular, this class defines `__getattribute__` so that attribute
    lookup as in `some_request.firstname` also returns a request, except for
    special methods: `some_request.__add__` will return the method. For
    consistency, assigning to request attributes is forbidden:
    `some_request.firstname = 'Alfred'` raises an exception.

    To access the actual attributes of Request objects, one needs to use
    `object.__getattribute__` and `object.__setattr__`.
    """
    # TODO: test `del some_request.fistname`. It should raise.
    def __getattribute__(self, name):
        if name.startswith('__') and name.endswith('__'):
            # Special methods such as __add__.
            # According to the following link CPython may not go through here
            # to get them, but there seems to be no guarantee that it does not.
            # http://docs.python.org/reference/datamodel.html#new-style-special-lookup
            return object.__getattribute__(self, name)
        else:
            return AttributeRequest(self, name)

    def __setattr__(self, name, value):
        raise AttributeError('Can not assign to request attributes.')


    def __getitem__(self, key):
        # No as_request() on key
        if isinstance(key, slice):
            return SliceRequest(self, key)
        elif isinstance(key, int):
            return IndexRequest(self, key)
        raise TypeError('getitem notation ("[]") is only supported for slice and integers')

    # Other magic methods are added later, at the bottom of this module.


class StoredItemsRequest(Request):
    """
    Represents the sequence of all items stored in a Storage
    """
    @self_with_attrs
    def __init__(self, storage):
        self.storage = storage


class LiteralRequest(Request):
    @self_with_attrs
    def __init__(self, value):
        self.value = value


class ListRequest(Request):
    @self_with_attrs
    def __init__(self, obj):
        self.value = [as_request(element) for element in obj]


class TupleRequest(Request):
    @self_with_attrs
    def __init__(self, obj):
        self.value = tuple(as_request(element) for element in obj)


class DictRequest(Request):
    @self_with_attrs
    def __init__(self, obj):
        self.value = dict(
            # TODO: what about fancy keys? (non-unicode or even Request)
            (key, as_request(value))
            for key, value in obj.iteritems())


class ContextRequest(Request):
    @self_with_attrs
    def __init__(self, scope_depth=0):
        scope_depth = int(scope_depth)
        if scope_depth > 0:
            # TODO better message
            raise ValueError('Depth must be negative or zero.')
        self.scope_depth = scope_depth

    @self_with_attrs
    def __call__(self, more_depth):
        more_depth = int(more_depth)
        if more_depth > 0:
            # TODO better message
            raise ValueError('Depth must be negative or zero.')
        return ContextRequest(self.scope_depth + more_depth)


class OperationRequest(Request):
    """
    Abstract base class for requests that are based on at least one other
    request. That "main" sub-request is in the `subject` attribute.
    """
    # For subclasses: name of the special method on Request object that return
    # this class.
    # Eg. Request.__add__ returns a AddRequest instance so
    # AddRequest.operator_name is 'add'
    operator_name = None

    @self_with_attrs
    def copy_replace(self, replace, replacement):
        newargs = []
        for arg_name in self.arg_spec:
            arg = getattr(self, arg_name)
            
            wrapper = WithRealAttributes(arg)
            if arg is replace:
                newargs.append(replacement)
            elif hasattr(wrapper, 'copy_replace'):
                newargs.append(wrapper.copy_replace(replace, replacement))
            else:
                newargs.append(arg)
        if hasattr(self, 'from_attributes'):
            return self.from_attributes(*newargs)
        else:
            return self.obj_type()(*newargs)


class UnaryOperationRequest(Request):
    """
    Abstract base class for request objects constructed with only one argument,
    another request object.
    
    Eg.  ~some_req is NegationRequest(some_req)
    """
    
    arg_spec = ('subject',)
    
    @self_with_attrs
    def __init__(self, subject):
        self.subject = subject


class BinaryOperationRequest(Request):
    """
    Abstract base class for request objects constructed with two arguments,
    both request objects.
    
    Eg.  some_req + other_req is AddRequest(some_req, other_req)
    """
    
    arg_spec = ('subject', 'other')
    
    @self_with_attrs
    def __init__(self, subject, other):
        self.subject = subject
        self.other = as_request(other)


# XXX when is __concat__ used instead of __add__?
# http://docs.python.org/library/operator.html?#operator.__concat__ says for
# sequences. Is that collection.Sequence?

# eg. `a + 1` is `a.__add__(1)`
# include these? abs, divmod
BINARY_OPERATORS = frozenset('''
    eq ne lt gt le ge
    add sub mul floordiv div truediv pow mod
    lshift rshift
    and or xor
    contains concat
'''.split())

UNARY_OPERATORS = frozenset('''
    pos neg invert
'''.split())


# eg. `1 + a` is `a.__radd__(1)`
REVERSED_OPERATORS = frozenset('''
    add sub mul floordiv div truediv mod pow
    lshift rshift
    and or xor
'''.split())

assert not (BINARY_OPERATORS & UNARY_OPERATORS)
assert REVERSED_OPERATORS < BINARY_OPERATORS # strict inclusion
OPERATORS = BINARY_OPERATORS | UNARY_OPERATORS


def _add_magic_method(operator_name):
    class_name = name.title() + 'Request'
    if operator_name in BINARY_OPERATORS:
        base_class = BinaryOperationRequest
    else:
        assert operator_name in UNARY_OPERATORS
        base_class = UnaryOperationRequest
    operation_class = type(class_name, (base_class,), {
        'operator_name': operator_name})
    # Add the new class in the scope of the current module, as if we had
    # written eg. a `class AddOperation(OperationRequest):` statement.
    setattr(sys.modules[__name__], class_name, operation_class)

    magic_name = '__%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if magic_name not in vars(Request):
        def magic_method(*args):
            # `*args` here includes `self`, the Request instance.
            return operation_class(*args)
        magic_method.__name__ = magic_name
        setattr(Request, magic_name, magic_method)

    magic_name = '__r%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if magic_name not in vars(Request) and name in REVERSED_OPERATORS:
        def magic_method(*args):
            # `*args` here includes `self`, the Request instance.
            args = args[::1]
            return operation_class(*args)
        magic_method.__name__ = magic_name
        setattr(Request, magic_name, magic_method)

for name in OPERATORS:
    _add_magic_method(name)

del name, _add_magic_method


class SliceRequest(OperationRequest):
    """
    some_req[4:-1] is SliceRequest(some_req, slice(4, -1, None))
    other_req[::2] is SliceRequest(other_req, slice(None, None, 2))
    """

    arg_spec = ('subject', 'slice')
    
    @self_with_attrs
    def __init__(self, subject, slice_):
        self.subject = subject
        self.slice = slice_  # No as_request() ?


class IndexRequest(OperationRequest):
    """
    some_req[4] is IndexRequest(some_req, 4)
    other_req[-1] is IndexRequest(other_req, -1)
    """

    arg_spec = ('subject', 'index')
    
    @self_with_attrs
    def __init__(self, subject, index):
        self.subject = subject
        self.index = index  # No as_request() ?


class AttributeRequest(OperationRequest):
    """
    some_req.firstname is AttributeRequest(req, 'firstname')

    Also has magic to implement methods on requests such as `some_req.one()`.
    """

    arg_spec = ('subject', 'attr_name')
    
    _methods = {}

    @classmethod
    def _register_method(cls, method_name):
        """
        Class decorator to register classes that implement methods on
        Request objects.
        """
        def decorator(class_):
            cls._methods[method_name] = class_
            return class_
        return decorator

    @self_with_attrs
    def __init__(self, subject, attr_name):
        self.subject = subject
        self.attr_name = attr_name  # No as_request()

    @self_with_attrs
    def __call__(self, *args, **kwargs):
        """
        Implement methods on requests:
        Replace eg. `GetattrRequest(s, 'map')(...)` by `MapRequest(s, ...))`
        """
        class_ = self._methods.get(self.attr_name, None)
        if class_ is None:
            raise TypeError('Request objects do not have a %s method.'
                            % self.attr_name)
        return class_(self.subject, *args, **kwargs)


ARGUMENT_NOT_GIVEN = object()

@AttributeRequest._register_method('one')
class OneRequest(OperationRequest):

    arg_spec = ('subject', 'default')
    
    @self_with_attrs
    def __init__(self, subject, default=ARGUMENT_NOT_GIVEN):
        self.subject = subject
        if default is ARGUMENT_NOT_GIVEN:
            self.default = None
        else:
            self.default = as_request(default)


@AttributeRequest._register_method('filter')
class FilterRequest(OperationRequest):

    arg_spec = ('subject', 'predicate')
    
    @self_with_attrs
    def __init__(self, subject, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        self.subject = subject

        if predicate is ARGUMENT_NOT_GIVEN:
            predicate = LiteralRequest(True)
        else:
            predicate = as_request(predicate)

        for name, value in kwargs.iteritems():
            predicate &= (getattr(ContextRequest(), name) == value)

        self.predicate = predicate


@AttributeRequest._register_method('map')
class MapRequest(OperationRequest):

    arg_spec = ('subject', 'new_value')
    
    @self_with_attrs
    def __init__(self, subject, new_value):
        self.subject = subject
        self.new_value = as_request(new_value)


@AttributeRequest._register_method('sort')
class SortRequest(OperationRequest):

    arg_spec = ('subject', 'sort_keys')
    
    @self_with_attrs
    def __init__(self, subject, *sort_keys):
        self.subject = subject
        if not sort_keys:
            # Default to comparing the element themselves, ie req.sort()
            # is the same as req.sort(CONTEXT)
            sort_keys = (ContextRequest(),)

        self.sort_keys = tuple()
        for sort_key in sort_keys:
            sort_key = as_request(sort_key)
            wrapped_sort_key = WithRealAttributes(sort_key)
            reverse = (getattr(wrapped_sort_key, 'operator_name', '')
                       == 'neg')
            if reverse:
                sort_key = wrapped_sort_key.subject
            
            self.sort_keys += ((sort_key, reverse),)
    
    @classmethod
    def from_attributes(cls, subject, sort_keys):
        return cls(subject, *(
            -key if reverse else key
            for key, reverse in sort_keys))


@AttributeRequest._register_method('groupby')
class GroupbyRequest(OperationRequest):

    arg_spec = ('subject', 'key')
    
    @self_with_attrs
    def __init__(self, subject, key):
        self.subject = subject
        self.key = as_request(key)


@AttributeRequest._register_method('sum')
class SumRequest(UnaryOperationRequest):
    pass


@AttributeRequest._register_method('min')
class MinRequest(UnaryOperationRequest):
    pass


@AttributeRequest._register_method('max')
class MaxRequest(UnaryOperationRequest):
    pass


@AttributeRequest._register_method('len')
class LenRequest(UnaryOperationRequest):
    pass


@AttributeRequest._register_method('distinct')
class DistinctRequest(UnaryOperationRequest):
    pass

