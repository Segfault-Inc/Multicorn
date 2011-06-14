# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import sys
import functools

# Marker to distinguish "Nothing was given" and "`None` was explicitly given".
ARGUMENT_NOT_GIVEN = object()


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

def as_chain(request):
    """Return a (wrapped) request as a chain of successive operations"""
    chain = [request]
    request = WithRealAttributes(request)
    if issubclass(request.obj_type(), OperationRequest):
        chain = as_chain(request.subject) + chain
    return chain

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
            # No as_request() on the name
            return AttributeRequest(self, name)

    def __setattr__(self, name, value):
        raise AttributeError('Can not assign to request attributes.')


    def __getitem__(self, key):
        # XXX No as_request() on key ?
        if isinstance(key, slice):
            return SliceRequest(self, key)
        elif isinstance(key, int):
            return IndexRequest(self, key)
        raise TypeError('getitem notation ("[]") is only supported for slice and integers')

    # XXX do we need this?
    def __pos__(self):
        """+some_req is some_req, a no-op."""
        return self

    def __neg__(self):
        return NegRequest(self)

    def __invert__(self):
        # Simplify logic when possible
        if isinstance(self, LiteralRequest):
            return LiteralRequest(not WithRealAttributes(self).value)
        else:
            return NotRequest(self)

    def __and__(self, other):
        other = as_request(other)
        # Simplify logic when possible
        for a, b in ((self, other), (other, self)):
            if isinstance(a, LiteralRequest):
                if WithRealAttributes(a).value:
                    # True is the neutral element of and
                    return b
                else:
                    # False is the absorbing element
                    return LiteralRequest(False)
        return AndRequest(self, other)

    def __or__(self, other):
        other = as_request(other)
        # Simplify logic when possible
        for a, b in ((self, other), (other, self)):
            if isinstance(a, LiteralRequest):
                if WithRealAttributes(a).value:
                    # True is the absorbing element of or
                    return LiteralRequest(True)
                else:
                    # False is the neutral element
                    return b
        return OrRequest(self, other)

    def __xor__(self, other):
        return (self & ~other) | (other & ~self)

    # `&`, `|` and `^` are commutative
    __rand__ = __and__
    __ror__ = __or__
    __rxor__ = __xor__

    @self_with_attrs
    def __repr__(self):
        name = self.obj_type().__name__
        assert name.endswith('Request')
        name = name[:-len('Request')]
        return '%s[%s]' % (name, ', '.join(
            repr(getattr(self, attr_name)) for attr_name in self.arg_spec))

    # Other magic methods are added later, at the bottom of this module.

    # Just like attributes, these methods can be accessed with eg.
    # `object.__getattribute__(some_req).map`, but AttributeRequest objects
    # are also callable so that `some_req.map(...)` Just Works™.
    def one(self, default=ARGUMENT_NOT_GIVEN):
        if default is ARGUMENT_NOT_GIVEN:
            default = None
        else:
            default = as_request(default)
        return OneRequest(self, default)

    def filter(self, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        if predicate is ARGUMENT_NOT_GIVEN:
            predicate = LiteralRequest(True)
        else:
            predicate = as_request(predicate)

        for name, value in kwargs.iteritems():
            predicate &= (getattr(ContextRequest(), name) == value)

        return FilterRequest(self, as_request(predicate))

    def map(self, new_value):
        return MapRequest(self, as_request(new_value))

    def execute(self):
        ap = WithRealAttributes(as_chain(self)[0]).storage
        return ap.execute(self)

    def sort(self, *sort_keys):
        if not sort_keys:
            # Default to comparing the element themselves, ie req.sort()
            # is the same as req.sort(CONTEXT)
            sort_keys = (ContextRequest(),)

        # If a sort_key is a negation (NegRequest), unwrap it and mark it as
        # "reverse", so that we can sort in the other direction for sort keys
        # that do not have a negative value (ie. non-numbers)
        decorated_sort_keys = []
        for sort_key in sort_keys:
            sort_key = as_request(sort_key)
            wrapped_sort_key = WithRealAttributes(sort_key)
            reverse = (getattr(wrapped_sort_key, 'operator_name', '')
                       == 'neg')
            if reverse:
                sort_key = wrapped_sort_key.subject

            decorated_sort_keys.append((sort_key, reverse))
        return SortRequest(self, decorated_sort_keys)

    def groupby(self, key):
        return GroupbyRequest(self, as_request(key))

    def sum(self):
        return SumRequest(self)

    def min(self):
        return MinRequest(self)

    def max(self):
        return MaxRequest(self)

    def len(self):
        return LenRequest(self)

    def distinct(self):
        return DistinctRequest(self)


class StoredItemsRequest(Request):
    """
    Represents the sequence of all items stored in a Storage
    """
    arg_spec = ('storage',)

    @self_with_attrs
    def __init__(self, storage):
        self.storage = storage

class LiteralRequest(Request):
    arg_spec = ('value',)

    @self_with_attrs
    def __init__(self, value):
        self.value = value

    @self_with_attrs
    def __repr__(self):
        return repr(self.value)


class ListRequest(Request):
    arg_spec = ('value',)

    @self_with_attrs
    def __init__(self, obj):
        self.value = [as_request(element) for element in obj]


class TupleRequest(Request):
    arg_spec = ('value',)

    @self_with_attrs
    def __init__(self, obj):
        self.value = tuple(as_request(element) for element in obj)


class DictRequest(Request):
    arg_spec = ('value',)

    @self_with_attrs
    def __init__(self, obj):
        self.value = dict(
            # TODO: what about fancy keys? (non-unicode or even Request)
            (key, as_request(value))
            for key, value in obj.iteritems())


class ContextRequest(Request):
    arg_spec = ('scope_depth',)

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
    # For subclasses: the name of the function in the `operator` module that
    # implement this operation, or None.
    # Eg. AddRequest.operator_name is 'add' so AddRequest(r1, r2) represents
    # `operator.__add__(v1, v2)` which is the same as `v1 + v2`, where r1 and
    # r2 respectively represent v1 and v2.
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
        return self.obj_type()(*newargs)


class UnaryOperationRequest(Request):
    """
    Abstract base class for request objects constructed with only one argument,
    another request object.

    Eg.  ~some_req is NotRequest(some_req)
    """

    arg_spec = ('subject',)

    @self_with_attrs
    def __init__(self, subject):
        self.subject = subject


class NotRequest(UnaryOperationRequest):
    """
    Logical negation:  ~some_req is NotRequest(some_req)
    """
    # Returned by Request.__invert__, but we really want it to be `not`, not
    # `invert`. There is no __not__ special method that we can override.
    operator_name = 'not'


class NegRequest(UnaryOperationRequest):
    """
    Arithmetic negation:  -some_req is NegRequest(some_req)
    """
    operator_name = 'neg'


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
        self.other = other


# XXX when is __concat__ used instead of __add__?
# http://docs.python.org/library/operator.html?#operator.__concat__ says for
# sequences. Is that collection.Sequence?

# eg. `a + 1` is `a.__add__(1)`
# include these? abs, divmod
BINARY_OPERATORS = frozenset('''
    eq ne lt gt le ge
    add sub mul floordiv div truediv pow mod
    and or
    contains concat
'''.split())

# eg. `1 + a` is `a.__radd__(1)`
REVERSED_OPERATORS = frozenset('''
    add sub mul floordiv div truediv mod pow
    and or
'''.split())

assert REVERSED_OPERATORS < BINARY_OPERATORS # strict inclusion


def _add_magic_method(operator_name):
    class_name = operator_name.title() + 'Request'
    operation_class = type(class_name, (BinaryOperationRequest,), {
        'operator_name': operator_name})
    # Add the new class in the scope of the current module, as if we had
    # written eg. a `class AddOperation(OperationRequest):` statement.
    setattr(sys.modules[__name__], class_name, operation_class)

    magic_name = '__%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if magic_name not in vars(Request):
        def magic_method(self, other):
            return operation_class(self, as_request(other))
        magic_method.__name__ = magic_name
        setattr(Request, magic_name, magic_method)

    magic_name = '__r%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if magic_name not in vars(Request) and operator_name in REVERSED_OPERATORS:
        def magic_method(self, other):
            # Swap arguments
            return operation_class(as_request(other), self)
        magic_method.__name__ = magic_name
        setattr(Request, magic_name, magic_method)

for name in BINARY_OPERATORS:
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
        self.slice = slice_


class IndexRequest(OperationRequest):
    """
    some_req[4] is IndexRequest(some_req, 4)
    other_req[-1] is IndexRequest(other_req, -1)
    """

    arg_spec = ('subject', 'index')

    @self_with_attrs
    def __init__(self, subject, index):
        self.subject = subject
        self.index = index


class AttributeRequest(OperationRequest):
    """
    some_req.firstname is AttributeRequest(req, 'firstname')

    Also has magic to implement methods on requests such as `some_req.one()`.
    """

    arg_spec = ('subject', 'attr_name')

    @self_with_attrs
    def __init__(self, subject, attr_name):
        self.subject = subject
        self.attr_name = attr_name

    @self_with_attrs
    def __call__(self, *args, **kwargs):
        """
        Implement methods on requests:
        eg. `some_req.map` is `GetattrRequest(some_req, 'map')`, but
        `some_req.map(...)` is `Request.map(some_req, ...)`.
        """
        method = getattr(WithRealAttributes(self.subject), self.attr_name, None)
        if method is None:
            raise TypeError('Request objects do not have a %s method.'
                            % self.attr_name)
        return method(*args, **kwargs)


class OneRequest(OperationRequest):
    arg_spec = ('subject', 'default')

    @self_with_attrs
    def __init__(self, subject, default):
        self.subject = subject
        self.default = default


class FilterRequest(OperationRequest):
    arg_spec = ('subject', 'predicate')

    @self_with_attrs
    def __init__(self, subject, predicate):
        self.subject = subject
        self.predicate = predicate


class MapRequest(OperationRequest):
    arg_spec = ('subject', 'new_value')

    @self_with_attrs
    def __init__(self, subject, new_value):
        self.subject = subject
        self.new_value = new_value


class SortRequest(OperationRequest):
    arg_spec = ('subject', 'sort_keys')

    @self_with_attrs
    def __init__(self, subject, sort_keys):
        self.subject = subject
        self.sort_keys = tuple(sort_keys)


class GroupbyRequest(OperationRequest):
    arg_spec = ('subject', 'key')

    @self_with_attrs
    def __init__(self, subject, key):
        self.subject = subject
        self.key = key


class SumRequest(UnaryOperationRequest):
    pass


class MinRequest(UnaryOperationRequest):
    pass


class MaxRequest(UnaryOperationRequest):
    pass


class LenRequest(UnaryOperationRequest):
    pass


class DistinctRequest(UnaryOperationRequest):
    pass
