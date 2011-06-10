# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
import attest

from .. import Multicorn
from .. import corns
from ..queries import Query, r


suite = Tests()


@suite.test
def test_memory_corn():
    mc = Multicorn()
    
    @mc.register
    @corns.Memory.declarative
    class foos:
        properties = {'hello': unicode, 'foo': int, 'buzziness': int}
        identity_properties = ['hello', 'foo']

    foos.create(dict(hello='World', foo=1, buzziness=0), save=True)
    foos.create(dict(hello='Lipsum', foo=1, buzziness=4), save=True)

    q = Query.sort(r.buzziness).select(fu=r.hello + '!')
    assert list(foos.search(q)) == [{'fu': 'World!'}, {'fu': 'Lipsum!'}]

    q = Query.where(hello='Lipsum')
    assert foos.open(q.select(b=r.buzziness)) == {'b': 4}

    item = foos.open(q)
    # This is an actual Item, not a dict.
    assert item.corn is foos
    
    
    # Make sure we're not doing a slow query
    class SlowQuery(Exception):
        pass
    def _all():
        # Fast queries should not go through here.
        raise SlowQuery()
    foos._all = _all
    
    with attest.raises(SlowQuery):
        # buzziness is not an identity property, this is a slow query
        foos.open(Query.where(buzziness=4))

    with attest.raises(SlowQuery):
        # foo is only part of the identity, this is a slow query
        foos.open(Query.where(foo=1))

    # Whole identity, this is a fast query
    assert foos.open(Query.where(hello='Lipsum', foo=1).select(b=r.buzziness)) \
        == {'b': 4}
