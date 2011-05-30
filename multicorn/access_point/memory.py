# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Memory
======

Access point storing items in memory. Mainly useful for testing.

"""

from . import AccessPoint


class Memory(AccessPoint, set):
    """Trivial access point that keeps everything in memory.

    Mainly useful for testing.

    """
    def search(self, request):
        return (item for item in self if request.test(item))

    def delete(self, item):
        self.remove(item)

    def delete_many(self, request):
        # build a temporary set as we can not delete (change the set size)
        # during iteration
        for item in set(self.search(request)):
            self.delete(item)

    def save(self, item):
        item.saved = True
        self.add(item)
