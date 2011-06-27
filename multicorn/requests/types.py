# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

class Type(object):

    def __init__(self,type=object, corn=None, name=None,):
        self.corn = corn
        self.name = name
        self.type = type

    def __repr__(self):
        return '%s(%r, %r, %r)' % (type(self).__name__, self.type, self.corn,
                                   self.name)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.corn == other.corn and\
                    self.name == other.name and\
                    self.type == other.type
        return False

    def __hash__(self):
        return hash((self.corn, self.name, self.type))

    def common_type(self, other):
        if self == other:
            return self
        if self.type == other.type:
            return Type(type=self.type)
        return Type(type=object)

class Dict(Type):

    def __init__(self, mapping=None,corn=None, name=None):
        super(Dict, self).__init__(corn=corn, name=name, type=dict)
        self.mapping = mapping

    def __repr__(self):
        return '%s(%r, %r, %r)' % (type(self).__name__, self.mapping,
                                   self.corn, self.name)
    def __eq__(self, other):
        return super(Dict, self).__eq__(other) and\
                self.mapping == other.mapping

    def __hash__(self):
        return hash((self.corn, self.name, self.type,
                     frozenset(self.mapping.items())))



class List(Type):

    def __init__(self, inner_type=Type(type=object), corn=None, name=None):
        super(List, self).__init__(corn=corn, name=name, type=list)
        self.inner_type = inner_type

    def __repr__(self):
        return '%s(%r, %r, %r)' % (type(self).__name__, self.inner_type,
                                   self.corn, self.name)
    def __eq__(self, other):
        return super(List, self).__eq__(other) and\
                self.inner_type == other.inner_type

    def __hash__(self):
        return hash((self.corn, self.name, self.type, self.inner_type))

    def common_type(self, other):
        if self == other:
            return self
        elif isinstance(other, List):
            return List(inner_type=self.inner_type.common_type(other.inner_type))
        else:
            return Type(type=object)
