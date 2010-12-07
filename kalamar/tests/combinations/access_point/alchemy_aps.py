from ..test_combinations import first_ap, second_ap
from kalamar.item import Item
from kalamar.access_point.alchemy import Alchemy, AlchemyProperty

URL = "sqlite:///"

@first_ap()
def make_first_ap():
    return Alchemy(URL, "first_ap", {
        "id": AlchemyProperty(int),
        "name": AlchemyProperty(unicode),
        "color": AlchemyProperty(unicode),
        "second_ap": AlchemyProperty(Item, relation="many-to-one",
            remote_ap="second_ap", remote_property="code")},
        ["id"], True)

@second_ap()
def make_second_ap():
    return Alchemy(URL, "second_ap", {
        "code": AlchemyProperty(unicode),
        "name": AlchemyProperty(unicode),
        "first_aps": AlchemyProperty(iter, relation="one-to-many",
            remote_ap="first_ap", remote_property="second_ap")},
        ["code"], True)
