# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .properties import *
from ..corns.extensers import AbstractCornExtenser


def declare(clazz, **kwargs):
    def build_corn(corn_definition):
        corn = clazz(corn_definition.__name__.lower(), **kwargs)
        bases = list(corn_definition.__bases__)
        # Contains
        wrappers = {}
        def append_wrapper(prop):
            if prop._wrapper:
                wrappers[prop._wrapper] = max(prop.depth,
                        wrappers.get(prop._wrapper, 0))
                if prop.wrapped_property:
                    append_wrapper(prop.wrapped_property)

        def find_prop(prop, wrapper_cls=None.__class__):
            if issubclass(wrapper_cls, prop._wrapper):
                return prop
            if prop.wrapped_property:
                return find_prop(prop.wrapped_property, wrapper_cls)
        props = {}
        for name in dir(corn_definition):
            prop = getattr(corn_definition, name)
            if isinstance(prop, Property):
                props[name] = prop
                append_wrapper(prop)
                prop = find_prop(prop)
                if prop:
                    corn.register(name, **prop.kwargs)

        for base in bases:
            if issubclass(base, AbstractCornExtenser):
                corn = base(corn.name, corn)
                for name, prop in props.items():
                    wrapped_prop = find_prop(prop, base)
                    if wrapped_prop:
                        corn.register(name, **wrapped_prop.kwargs)
        return corn
    return build_corn
