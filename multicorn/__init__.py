# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
import logging


class Multicorn(object):
    def __init__(self, quiet=True):
        self.corns = {}
        self.log = logging.getLogger('multicorn')

        class NullHandler(logging.Handler):
            """Handler that do nothing"""
            def emit(self, record):
                """Do nothing"""

        handler = NullHandler()
        if not quiet:
            self.log.setLevel(logging.WARN)
            try:
                from log_colorizer import make_colored_stream_handler
                handler = make_colored_stream_handler()
            except ImportError:
                pass

        self.log.addHandler(handler)

    def register(self, corn):
        corn.bind(self)  # Do this first as it may raise.
        corns = list(self.corns.values())
        self.corns[corn.name] = corn
        for oldcorn in corns:
            oldcorn.registration()
        return corn
