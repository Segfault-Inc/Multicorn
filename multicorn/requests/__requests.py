# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import operator


def _ensure_request(obj):
    """
    Ensure that the given parameter is an Request. If it is not, wrap it
    in a Literal.
    """
    if isinstance(obj, Request):
        return request
    elif isinstance(obj, list):
        return [_ensure_request(element) for element in obj]
    elif isinstance(request, tuple):
        return tuple(_ensure_request(element) for element in obj)
    elif isinstance(obj, tuple):
        return dict(
            # TODO: what about fancy keys? (non-unicode or even Request)
            (key, _ensure_request(value))
            for key, value in obj.iteritems())
    else:
        return Literal(obj)


class StateHolder(object):
    """
    Any real attribute on Request subclasses would block __getattr__ and
    conflict with eg. `r.name`.
    To avoid these conflicts, any state is kept in a single "private" attribute
    named `__state`.
    """
    pass


class Request(object):
    """
    Abstract base class for all requests.
    """
    def __getattr__(self, name):
        return Operation('getattr', self, name)
        
    def __getitem__(self, key):
        return Operation('getitem', self, name)

    # Since bool inherits from int, `&` and `|` (bitwise `and` and `or`)
    # behave as expected on booleans

    # Magic method for `self & other`, not `self and other`
    def __and__(self, other):
        other = _ensure_request(other)
        # Simplify logic when possible
        for a, b in ((self, other), (other, self)):
            if isinstance(a, Literal):
                if a._Literal__state.value:
                    return b # True is the neutral element of and
                else:
                    return Literal(False) # False is the absorbing element
        return Operation('and', self, other)
    
    # Magic method for `self | other`, not `self or other`
    def __or__(self, other):
        other = _ensure_request(other)
        # Simplify logic when possible
        for a, b in ((self, other), (other, self)):
            if isinstance(a, Literal):
                if a._Literal__state.value:
                    return Literal(True) # True is the absorbing element of or
                else:
                    return b # False is the neutral element
        return Operation('or', self, other)

    # `&` and `|` are commutative
    __rand__ = __and__
    __ror__ = __or__
    
    # However `~` (bitwise invert) does not behave as a logical `not`
    # on booleans:
    #   (~True, ~False) == (~1, ~0) == (-2, -1)
    # We want to use `~` on requests as the logical `not`.
    def __invert__(self):
        # Simplify logic when possible
        if isinstance(self, Literal):
            return Literal(not self._Literal__state.value)
        # Use `not` instead of `invert` here:
        return Operation('not', self)
    
    @classmethod
    def _add_magic_method(cls, operator_name, reverse=False):
        method_name = '__%s%s__' % (('r' if reverse else ''), operator_name)
        # Define the method inside a function so that the closure will hold
        # each value of `operator_name` and `reverse`
        def magic_method(*args):
            # `*args` here includes the method’s `self` so that the following
            # works:
            if reverse:
                args = args[::-1]
            return Operation(operator_name, *(
                _ensure_request(arg) for arg in args))
        magic_method.__name__ = method_name
        setattr(cls, method_name, magic_method)

# Dynamically add methods to Request.
# Include these? abs, index, divmod
for names, reverse in (
        ('''lt le eq ne ge gt add concat contains div floordiv lshift mod mul
            neg pos pow rshift sub truediv''', False),
        # Reversed operators: eg. 1 + r.foo => r.foo.__radd__(1)
        ('''add sub mul floordiv div truediv mod pow lshift rshift''', True)):
    for name in names.split():
        Request._add_magic_method(name, reverse)


class Literal(Request):
    def __init__(self, value):
        self.__state = StateHolder() # See StateHolder's docstring
        self.__state.value = value

    def __repr__(self):
        return repr(self.__state.value)
#        return 'Lit(%r)' % (self.__state.value,)


class Operation(Request):
    def __init__(self, operator_name, *args):
        self.__state = StateHolder() # See StateHolder's docstring
        self.__state.operator_name = operator_name
        self.__state.args = args

    def __repr__(self):
        # Make a list to avoid the trailing comma in one-element tuples.
        return 'Op(%s, %s)' % (self.__state.operator_name,
                               repr(list(self.__state.args))[1:-1])


class GetattrOperation(Operation):
    def __call__(self, *args, **kwargs):
        class_ = getattr(TransformationClasses, self.__state.name, None)
        if class_ is None:
            raise TypeError('Request objects do not have a %s method.'
                            % self.__state.name) 
        transformation = class_(*args, **kwargs)
        return TransformationChain(self.__state.subject, transformation)


class Root(Request):
    def __init__(self, scope_depth=0):
        assert scope_depth <= 0 # TODO message
        self.__state = StateHolder() # See StateHolder's docstring
        self.__state.scope_depth = int(scope_depth)

    def __getitem__(self, more_depth):
        return Root(self.__state.scope_depth + int(more_depth))


class TransformationChain(Request):
    def __init__(self, subject, transformation):
        self.__state = StateHolder() # See StateHolder's docstring
        transformations = (transformation,)
        if isinstance(subject, TransformationChain):
            transformations = subject.transformations + transformations
            subject = subject.subject
        self.__state.subject = subject
        self.__state.transformations = transformations


class Transformation(object): # Not an Request
    pass


class MethodOperations:
    class FilterOperation(Operation):
        def __init__(*args, **kwargs):
            if len(arg) == 1:
            predicate = predicate

    class map(Transformation):
        def __init__(self, new_element):
            self.new_element = new_item

    class sort(Transformation):
        def __init__(self, sort_key):
            self.sort_key = sort_key

    class groupby(Transformation):
        def __init__(self, group_key):
            self.group_key = group_key

    class distinct(Transformation):
        pass

    class max(Transformation):
        pass

    class min(Transformation):
        pass

    class count(Transformation):
        pass

    class sum(Transformation):
        pass

