# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ..corns.extensers.typeextenser import TypeExtenser
from ..corns.extensers.computed import ComputedExtenser, RelationExtenser
from functools import wraps
from ..requests import CONTEXT as c
from ..requests import requests


class Property(object):
    _wrapper = None

    def __init__(self, property=None, **kwargs):
        self.wrapped_property = property
        self.kwargs = kwargs

    @property
    def depth(self):
        if self.wrapped_property:
            return self.wrapped_property.depth + 1
        else:
            return 0


class TypeProperty(Property):
    _wrapper = TypeExtenser

class ComputedProperty(Property):
    _wrapper = ComputedExtenser

    def reverse(self, fun):
        self.kwargs['reverse'] = fun(self)
        return self

class DateProperty(TypeProperty):
    pass


class TextProperty(TypeProperty):
    pass

class computed(object):

    def __init__(self):
        pass

    def __call__(self, fun):
        kwargs = {'expression': fun}
        return ComputedProperty(**kwargs)

class Relation(ComputedProperty):

    _wrapper = RelationExtenser

    def __init__(self, to=None, on=None, uses=None, reverse_suffix='s'):
        super(Relation, self).__init__(property=None, to=to, on=on, uses=uses, reverse_suffix=reverse_suffix)
