from ..test_combinations import first_ap, second_ap
from kalamar.item import Item
from kalamar.property import Property
from kalamar.access_point.memory import Memory

@first_ap()
def make_first_ap():
    return Memory({
        "id": Property(int),
        "name": Property(unicode),
        "color": Property(unicode),
        "second_ap": Property(Item, relation="many-to-one",
            remote_ap="second_ap", remote_property="code")},
        ["id"])

@second_ap()
def make_second_ap():
    return Memory({
        "code": Property(unicode),
        "name": Property(unicode),
        "first_aps": Property(iter, relation="one-to-many",
            remote_ap="first_ap", remote_property="second_ap")},
        ["code"])
