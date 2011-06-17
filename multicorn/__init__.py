# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

# If pygment installed use colorize
try:
    from pygments.console import colorize
except ImportError:
    colorize = lambda x, y: y


class Multicorn(object):
    def __init__(self):
        self.corns = {}

    def register(self, corn):
        corn.bind(self)  # Do this first as it may raise.
        self.corns[corn.name] = corn
        return corn
