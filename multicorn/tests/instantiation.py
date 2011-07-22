# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
import attest


from multicorn.corns.memory import Memory
from multicorn.requests.types import Type
from multicorn import Multicorn
from multicorn.requests import CONTEXT as c
from multicorn.declarative import declare, Property, computed, Relation
from multicorn.corns.extensers.computed import ComputedExtenser

suite = Tests()


@suite.test
def test_simple_instantiation():
    mc = Multicorn()
    memory_corn = Memory(name="memory", identity_properties=("id",))
    memory_corn.register("id")
    memory_corn.register("name")
    mc.register(memory_corn)


@suite.test
def test_declarative():
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property()
        name = Property()
    assert isinstance(Corn.wrapped_corn, Memory)
    assert "name" in Corn.properties
    assert "id" in Corn.properties
    assert Corn.identity_properties == ("id",)
    name = Corn.properties["name"]
    assert isinstance(name, Type)
    assert name.type == object
    assert name.corn == Corn
    assert name.name == "name"

@suite.test
def test_wrapper_declaration():
    mc = Multicorn()
    @mc.register
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        foreign_id = Property(type=int)
        @computed()
        def foreign(self):
            return self.all.filter(c.id == c(-1).foreign_id).one(None)

        @foreign.reverse
        def foreign(self):
            return {'foreign_id': lambda item: item['foreign']['id'] if item['foreign'] else None}

    assert isinstance(Corn, ComputedExtenser)
    assert "name" in Corn.properties
    assert "id" in Corn.properties
    assert "foreign" in Corn.properties
    assert hasattr(Corn.properties['foreign'], 'expression')
    assert Corn.identity_properties == ("id",)

    item1 = Corn.create({'id': 1, 'name': 'foo'})
    item1.save()
    item2 = Corn.create({'id': 2, 'name': 'bar', 'foreign': item1})
    item2.save()
    item2 = Corn.all.filter(c.id == 2).one().execute()
    assert item2['foreign'] == item1
    item2 = Corn.all.filter(c.foreign.name == 'foo').one().execute()
    assert item2['id'] == 2
    assert item2['foreign'] == item1

@suite.test
def test_relation_declaration():
    mc = Multicorn()
    @mc.register
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        foreign = Relation(to="corn")
    assert isinstance(Corn, ComputedExtenser)
    assert "name" in Corn.properties
    assert "id" in Corn.properties
    assert "foreign" in Corn.properties
    assert hasattr(Corn.properties['foreign'], 'expression')
    assert Corn.identity_properties == ("id",)

    item1 = Corn.create({'id': 1, 'name': 'foo'})
    item1.save()
    item2 = Corn.create({'id': 2, 'name': 'bar', 'foreign': item1})
    item2.save()
    #item2 = Corn.all.filter(c.id == 2).one().execute()
    #assert item2['foreign'] == item1
    item2 = Corn.all.filter(c.foreign.name == 'foo').one().execute()
    assert item2['id'] == 2
    assert item2['foreign'] == item1
    item3 = Corn.create({'id': 3, 'name': 'foo', 'foreign': 1})
    item3.save()
    items = list(Corn.all.filter(c.foreign.name == 'foo').map(c.foreign.id).sort(c).execute())
    assert len(items) == 2
    assert all(x == 1 for x in items)
