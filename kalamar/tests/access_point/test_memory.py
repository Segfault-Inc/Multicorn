# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Memory test.

Test the Memory access point.

"""

from kalamar.access_point.memory import Memory
from kalamar.property import Property

from ..common import run_common



def make_ap():
    """Create a simple access point."""
    return Memory({"id": Property(int), "name": Property(unicode)}, ("id",))

@run_common
def test_common():
    """Launch common tests for memory."""
    return make_ap()
