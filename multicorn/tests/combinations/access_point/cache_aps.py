# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for Cache access point combinations.

"""

from multicorn.access_point.cache import Cache

from ..test_combinations import FirstWrapper, SecondWrapper


@FirstWrapper()
@SecondWrapper()
def make_cache(access_point):
    """Wrap ``access_point`` with a cache."""
    return lambda: Cache(access_point())
