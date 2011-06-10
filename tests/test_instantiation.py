from multicorn.corns.memory import Memory
from multicorn import Multicorn


def test_simple_instantiation():
    mc = Multicorn()
    memory_corn = Memory(name="memory", identity_properties=("id",))
    memory_corn.register("id")
    memory_corn.register("name")
    mc.register(memory_corn)




