# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from attest import Tests, assert_hook
from multicorn.utils import colorize
from multicorn.corns.mongo import Mongo
from multicorn.declarative import declare, Property
from . import make_test_suite


def make_corn():
    @declare(Mongo, identity_properties=("id",),
                 hostname="localhost", port=27017,
                 database="dbtst", collection="mctest")
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


def teardown(corn):
    #Deleting all objects the hardcore way
    corn.db.drop_collection(corn.collection)

try:
    import pymongo
except ImportError:
    import sys
    print(colorize(
        'yellow',
        "WARNING: The Mongo DB AP is not tested."), file=sys.stderr)
    suite = Tests()
else:
    suite = make_test_suite(make_corn, 'mongo', teardown)
