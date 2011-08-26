# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from attest import assert_hook
from multicorn.requests import CONTEXT as c
from multicorn.utils import colorize
from functools import wraps
from time import time


TESTS = []


def corntest(fun):
    TESTS.append(fun)
    return fun


class timed(object):
    def __init__(self, corn):
        self.corn = type(corn).__name__

    def __call__(self, f):

        @wraps(f)
        def wrapper(*arg, **kwargs):
            t = time()
            ret = f(*arg, **kwargs)
            total = time() - t
            print(
                colorize("yellow", self.corn) +
                colorize("white", "]  \t") +
                colorize("blue", f.__name__) +
                colorize("white", '\t -> \t') +
                colorize("red", '%0.3f ms' % total))
            return ret
        return wrapper


@corntest
def robustness(Corn):
    """ Tests if the corn is robust """
    size = 10000

    @timed(Corn)
    def mass_insert():
        items = (Corn.create({
                'id': i,
                'name': u'n %d' % i,
                'lastname': u'ln %d' % i}) for i in xrange(size))
        Corn.save(*items)
    mass_insert()

    @timed(Corn)
    def count_items():
        return Corn.all.len().execute()
    length = count_items()
    assert length == size

    @timed(Corn)
    def one_item():
        return Corn.all.filter(
            c.name == u"n %d" % (size / 2)).one().execute()
    item = one_item()
    assert item["id"] == size / 2

    @timed(Corn)
    def several_items_g():
        return Corn.all.filter(
            (c.id > size / 100) & (c.id < size / 10)).execute()

    @timed(Corn)
    def several_items_l():
        return list(Corn.all.filter(
            (c.id > size / 100) & (c.id < size / 10)).execute())

    assert several_items_l() == list(several_items_g())

    @timed(Corn)
    def map_alias():
        return list(Corn.all.map({
            'ln': c.lastname,
            'n': c.name,
            'i': c.id}).execute())

    map_alias()

    @timed(Corn)
    def map_alias_comb():
        return list(Corn.all.map(c + {
            'fn': c.name + u' ' + c.lastname}).execute())

    map_alias_comb()

    @timed(Corn)
    def map_square():
        return list(Corn.all.map(c + {'square': c.id * c.id}).execute())

    map_square()

    @timed(Corn)
    def map_agg_max():
        return Corn.all.max(c.id).execute()

    assert map_agg_max() == size - 1

    @timed(Corn)
    def map_agg_min():
        return Corn.all.min(c.id).execute()

    assert map_agg_min() == 0

    @timed(Corn)
    def map_agg_sum():
        return Corn.all.sum(c.id).execute()

    assert map_agg_sum() == sum(xrange(size))

    @timed(Corn)
    def map_groupby():
        return list(Corn.all.groupby(
            c.name[:3], max_=c.max(c.id), min_=c.min(c.id),
            sum_=c.sum(c.id)).sort(c.key).execute())

    assert len(map_groupby()) == 10
