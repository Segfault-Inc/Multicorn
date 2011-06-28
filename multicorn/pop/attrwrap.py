# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from collections import MutableMapping


class AttrWrap(MutableMapping):

    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        return self.wrapped[name]

    def __setattr__(self, name, value):
        if name == "wrapped":
            super(AttrWrap, self).__setattr__(name, value)
        else:
            self.wrapped[name] = value

    def __repr__(self):
        return self.wrapped.__repr__()

    def __len__(self):
        return self.wrapped.__len__()

    def __iter__(self):
        return self.wrapped.__iter__()

    def __contains__(self, key):
        return self.wrapped.__contains__(key)

    def __getitem__(self, key):
        return self.wrapped.__getitem__(key)

    def __setitem__(self, key, value):
        return self.wrapped.__setitem__(key, value)

    def __delitem__(self, key):
        return self.wrapped.__delitem__(key)

    def save(self):
        return self.wrapped.save()

    def delete(self):
        return self.wrapped.delete()
