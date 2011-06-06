# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .expressions import _ensure_expression


class By(object):
    def __init__(self, **criteria):
        self.criteria = criteria


class AggregateFunction(object):
    'Abstract'
    def __init__(self, expression):
        self.expression = _ensure_expression(expression)

class Sum(AggregateFunction): pass
class Min(AggregateFunction): pass
class Max(AggregateFunction): pass
class Count(AggregateFunction): pass
class Avg(AggregateFunction): pass

