# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import sys


def ensure_request(obj):
    """
    * If obj is a Request, return as-is.
    * If it is a list, tuple or dict, apply recursively to all elements.
    * Otherwise, wrap in a Literal.
    """
    if isinstance(obj, Request):
        return request
    elif isinstance(obj, list):
        return [ensure_request(element) for element in obj]
    elif isinstance(request, tuple):
        return tuple(ensure_request(element) for element in obj)
    elif isinstance(obj, tuple):
        return dict(
            # TODO: what about fancy keys? (non-unicode or even Request)
            (key, ensure_request(value))
            for key, value in obj.iteritems())
    else:
        return Literal(obj)


class Request(object):
    def __init__(self, *args):
        self.__args = args

    # Magic methods are added later


class Literal(Request):
    def __init__(self, value): # only one argument
        super(Literal, self).__init__(value)


class Root(Request): # TODO better name
    def __init__(self, scope_depth=0):
        if scope_depth > 0:
            # TODO better message
            raise ValueError('scope_depth must be negative')
        super(Root, self).__init__(int(scope_depth))

    def __call__(self, more_depth):
        scope_depth, = self._Request__args
        return Root(scope_depth + int(more_depth))


class Operation(Request):
    pass


class REQUEST_METHODS:
    # Namespace class, not meant to be instantiated
    # These just pre-process the arguments given to a method and return
    # a tuple of arguments to instantiate *Operation classes.
    
    def filter(*args, **kwargs):
        if not args:
            predicate = Literal(True)
        elif len(args) == 1:
            predicate, = args
            predicate = ensure_request(predicate)
        else:
            raise TypeError('filter takes at most one positional argument.')
        for name, value in kwargs.iteritems():
            predicate &= (getattr(Root(), name) == value)
        return (predicate,)
    
    def map(new_item):
        return (ensure_request(new_item),)

    def sort(sort_key):
        return (ensure_request(sort_key),)

    def group_by(group_key):
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
        class_name = '%sOperation' % name.title()
        class_ = type(class_name, (Operation,), {})
        # Add the new class in the scope of the current module, as if we had
        # written eg. a `class AddOperation(Operation):` statement.
        setattr(sys.modules[__name__], class_name, class_)
        registry[name] = class_

del names, registry, name


class GetattrOperation(Operation):
    def __call__(*args, **kwargs):
        """
        Implement methods on requests:
        Replace eg. `GetattrOperation(s, 'map')` by `MapOperation(s, ...)`
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

OPERATION_CLASS_BY_OPERATOR_NAME['getattr'] = GetattrOperation


# Add magic methods to Request
def _add_magic_method(operator_name):
    operation_class = OPERATION_CLASS_BY_OPERATOR_NAME[operator_name]
    
    magic_name = '__%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if not hasattr(Request, magic_name):
        def magic_method(*args):
            # `*args` here includes `self`, the Request instance.
            return operation_class(*(ensure_request(arg) for arg in args))
        setattr(Request, magic_name, magic_method)

    magic_name = '__r%s__' % operator_name
    # Only generate a magic method if it is not there already.
    if not hasattr(Request, magic_name) and name in REVERSED_OPERATORS:
        def magic_method(*args):
            # `*args` here includes `self`, the Request instance.
            args = args[::1]
            return operation_class(*args)
        setattr(Request, magic_name, magic_method)

for name in OPERATORS:
    _add_magic_method(name)

del name, _add_magic_method

