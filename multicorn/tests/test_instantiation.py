from attest import Tests, assert_hook
import attest


from multicorn.corns.memory import Memory
from multicorn.corns.abstract import Type
from multicorn import Multicorn
from multicorn.declarative import declare, Property

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
    assert isinstance(Corn, Memory)
    assert "name" in Corn.properties
    assert "id" in Corn.properties
    assert Corn.identity_properties == ("id",)
    name = Corn.properties["name"]
    assert isinstance(name, Type)
    assert name.type == object
    assert name.corn == Corn
    assert name.name == "name"
