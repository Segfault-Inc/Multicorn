from ..test_combinations import first_wrapper, second_wrapper
from kalamar.item import Item
from kalamar.access_point.cache import Cache


@first_wrapper()
@second_wrapper()
def make_cache(ap):
    return lambda : Cache(ap())


