# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Multicorn - Content Management Library
======================================

Multicorn offers tools to access, create and edit data stored in heterogeneous
storage engines, with a simple key/value interface.

The storage engines are called "access points". Some access points map common
low-level storages such as databases or filesystems. Other access points can
manage, atop low-level access points, high-level features such as cache, keys
aliases or data formats.

Different access points can be listed in a Multicorn "site", and relations can
be defined between these access points, just as in a relational database. This
mechanism automatically links the items stored in the linked access points,
enabling the user to easily use joins if needed.

"""

if "unicode" in __builtins__:
    # Define ``bytes`` for Python 2
    __builtins__["bytes"] = str
else:
    # Define ``unicode`` and ``basestring`` for Python 3
    __builtins__["unicode"] = __builtins__["basestring"] = str
