# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class RageQuit(Exception):

    def __init__(self, request, message=""):
        self.request = request
        self.message = message

    def __repr__(self):
        return "RageQuit(request=%r, message=%r)" % (self.request, self.message)
