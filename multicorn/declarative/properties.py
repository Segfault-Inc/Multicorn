# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ..corns.extensers.typeextenser import TypeExtenser
from ..corns.extensers.computed import ComputedExtenser
from functools import wraps
from ..requests import CONTEXT as c
from ..requests import requests


class Property():
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

    def __init__(self, on=None):
        self.on = on

    def __call__(self, fun):
        kwargs = {'expression': fun}
        if self.on:
            kwargs['property'] = self.on
        return ComputedProperty(**kwargs)

class Relation(ComputedProperty):

    def __init__(self, remote_ap, self_property=None):
        if self_property is None:
            if isinstance(remote_ap, basestring):
                self_property = Property(name=remote_ap)
            else:
                self_property = Property(name=remote_ap.name, type=remote_ap.identity_properties(1))
        def foreign(self):
            if isinstance(remote_ap, basestring):
                real_ap = self.multicorn.corns[remote_ap]
            else:
                real_ap = remote_ap
            if len(real_ap.identity_properties) != 1:
                raise KeyError("Unable to build relationship: real_ap has more"
                        "than one identity properties")
            remote_attr = requests.AttributeRequest(subject=c,
                attr_name=real_ap.identity_properties[0])
            self_attr = requests.AttributeRequest(subject=c(-1), attr_name=self_property)
            return remote_ap.all.filter(remote_attr == self_attr)
