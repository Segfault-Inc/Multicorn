# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import sys


def ensure_request(obj):
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


class Request(object):
    def __init__(self, *args):
        self.__args = args
    
    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            # Use list() to avoid the trailing comma in
            # one-element tuples.
            repr(list(self.__args))[1:-1])
    
    def __getattr__(self, name):
        # No ensure_request() on name
        return GetattrRequest(self, name)

    def __getitem__(self, key):
        # No ensure_request() on key
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


class LiteralRequest(Request):
    pass


class ListRequest(Request):
    def __init__(self, obj):
        super(ListRequest, self).__init__(
            [ensure_request(element) for element in obj])


class TupleRequest(Request):
    def __init__(self, obj):
        super(TupleRequest, self).__init__(
            tuple(ensure_request(element) for element in obj))


class DictRequest(Request):
    def __init__(self, obj):
        super(DictRequest, self).__init__(dict(
            # TODO: what about fancy keys? (non-unicode or even Request)
            (key, ensure_request(value))
            for key, value in obj.iteritems()))


class ContextRequest(Request):
    def __init__(self, scope_depth=0):
        super(ContextRequest, self).__init__(int(scope_depth))

    def __call__(self, more_depth):
        more_depth = int(more_depth)
        if more_depth > 0:
            # TODO better message
            raise ValueError('depth must be negative')
        scope_depth, = self._Request__args
        return ContextRequest(scope_depth + more_depth)


class OperationRequest(Request):
    pass


ARGUMENT_NOT_GIVEN = object()

class REQUEST_METHODS:
    # Namespace class, not meant to be instantiated
    # These just pre-process the arguments given to a method and return
    # a tuple of arguments to instantiate *Operation classes.

    def one(default=ARGUMENT_NOT_GIVEN):
        if default is ARGUMENT_NOT_GIVEN:
            default = None
        else:
            default = ensure_request(default)
        return (default,)

    def filter(*args, **kwargs):
        if not args:
            predicate = LiteralRequest(True)
        elif len(args) == 1:
            predicate, = args
            predicate = ensure_request(predicate)
        else:
            raise TypeError('filter takes at most one positional argument.')
        for name, value in kwargs.iteritems():
            predicate &= (getattr(ContextRequest(), name) == value)
        return (predicate,)

    def map(new_item):
        return (ensure_request(new_item),)

    def sort(*sort_keys):
        if not sort_keys:
            # Default to comparing the element themselves, ie req.sort()
            # is the same as req.sort(CONTEXT)
            sort_keys = (ContextRequest(),)
        return tuple(ensure_request(key) for key in sort_keys)

    def groupby(group_key):
        return (ensure_request(group_key),)

    # () is the empty tuple
    def sum(): return ()
    def min(): return ()
    def max(): return ()
    def len(): return ()
    def distinct(): return ()

REQUEST_METHODS = REQUEST_METHODS.__dict__
REQUEST_METHOD_NAMES = frozenset(REQUEST_METHODS)


# XXX when is __concat__ used instead of __add__?
# http://docs.python.org/library/operator.html?#operator.__concat__ says for
# sequences. Is that collection.Sequence?

# eg. `a + 1` is `a.__add__(1)`
# include these? abs, divmod
OPERATORS = frozenset('''
    eq ne lt gt le ge
    pos neg invert
    add sub mul floordiv div truediv pow mod
    lshift rshift
    and or xor
    contains concat
    getattr getitem
'''.split())

# eg. `1 + a` is `a.__radd__(1)`
REVERSED_OPERATORS = frozenset('''
    add sub mul floordiv div truediv mod pow
    lshift rshift
    and or xor
'''.split())

assert REVERSED_OPERATORS < OPERATORS # strict inclusion
assert not (OPERATORS & REQUEST_METHOD_NAMES)


OPERATION_CLASS_BY_OPERATOR_NAME = {}
OPERATION_CLASS_BY_METHOD_NAME = {}

for names, registry in (
        (OPERATORS, OPERATION_CLASS_BY_OPERATOR_NAME),
        (REQUEST_METHOD_NAMES, OPERATION_CLASS_BY_METHOD_NAME)):
    for name in names:
        class_name = name.title() + 'Request'
        class_ = type(class_name, (OperationRequest,), {})
        # Add the new class in the scope of the current module, as if we had
        # written eg. a `class AddOperation(OperationRequest):` statement.
        setattr(sys.modules[__name__], class_name, class_)
        registry[name] = class_

del names, registry, name

class SliceRequest(OperationRequest):
    pass

class IndexRequest(OperationRequest):
    pass

# A GetattrRequest class was generated above, but override with this one
# that has a __call__
class GetattrRequest(OperationRequest):
    def __call__(*args, **kwargs):
        """
        Implement methods on requests:
        Replace eg. `GetattrRequest(s, 'map')(...)` by
        `MapRequest(s, *REQUEST_METHODS['map'](...))`
        """
        if not args:
            raise TypeError("No positional argument given for 'self'.")
        self = args[0]
        args = args[1:]

        subject, attr_name = self._Request__args

        preprocessor = REQUEST_METHODS.get(attr_name, None)
        if preprocessor is None:
            raise TypeError('Request objects do not have a %s method.'
                            % attr_name)
        class_ = OPERATION_CLASS_BY_METHOD_NAME[attr_name]
        args = preprocessor(*args, **kwargs)
        return class_(subject, *args)

OPERATION_CLASS_BY_OPERATOR_NAME['getattr'] = GetattrRequest
OPERATION_CLASS_BY_METHOD_NAME['index'] = IndexRequest
OPERATION_CLASS_BY_METHOD_NAME['slice'] = SliceRequest


# Add magic methods to Request
def _add_magic_method(operator_name):
    operation_class = OPERATION_CLASS_BY_OPERATOR_NAME[operator_name]

    magic_name = '__%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if magic_name not in vars(Request):
        def magic_method(*args):
            # `*args` here includes `self`, the Request instance.
            return operation_class(*(ensure_request(arg) for arg in args))
        setattr(Request, magic_name, magic_method)

    magic_name = '__r%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if magic_name not in vars(Request) and name in REVERSED_OPERATORS:
        def magic_method(*args):
            # `*args` here includes `self`, the Request instance.
            args = args[::1]
            return operation_class(*args)
        setattr(Request, magic_name, magic_method)

for name in OPERATORS:
    _add_magic_method(name)

del name, _add_magic_method


OPERATOR_NAME_BY_OPERATION_CLASS = dict((v, k) for k, v in
    OPERATION_CLASS_BY_OPERATOR_NAME.iteritems())

METHOD_NAME_BY_OPERATION_CLASS = dict((v, k) for k, v in
    OPERATION_CLASS_BY_METHOD_NAME.iteritems())


