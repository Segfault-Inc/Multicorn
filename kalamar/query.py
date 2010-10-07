import itertools
from operator import itemgetter
from kalamar.request import Condition, make_request_property
from kalamar.item import Item

class Query(object):
    pass

class QueryChain(object):
    """Chained query
    
    >>> items = itertools.cycle([{'a':1, 'b':1},{'a':2, 'b': 2}])
    >>> range = QueryRange(slice(1,4))
    >>> cond = QueryFilter(Condition("a", "=", 2))
    >>> chain = QueryChain([range, cond])
    >>> list(chain(items))
    [{'a': 2, 'b': 2}, {'a': 2, 'b': 2}]
    >>> chain = QueryChain([chain, QueryDistinct()])
    >>> list(chain(items))
    [{'a': 2, 'b': 2}]

    """

    def __init__(self, queries):
        self.queries = queries

    def __call__(self, items):
        for query in self.queries:
             items = query(items)
        for item in items:
            yield item


class QueryDistinct(Query):
    """Returns a set of elements

    >>> items = [{'a': 1, 'b': 2}, {'a':2, 'b': 3}, {'a': 1, 'b': 2}]
    >>> list(QueryDistinct()(items))
    [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}]

    """

    def __init__(self):
        pass

    def __call__(self, items):
        tuples = tuple([tuple(item.items()) for item in items])
        for item in set(tuples):
            yield dict(item)
            

class QueryFilter(Query):
    """Filter a set of items
    
    >>> cond = Condition("a", "=" , 12)
    >>> items = [ {"a":13, "b" : 15}, {"a": 12, "b" : 16}]
    >>> filter = QueryFilter(cond)
    >>> list(filter(items))
    [{'a': 12, 'b': 16}]

    """

    def __init__(self, condition):
        self.condition = condition

    def __call__(self, items):
        for item in items:
            if self.condition.test(item):
                yield item

    
class QueryOrder(Query):
    """Order a set of items

    >>> items = [{"a" : 4, "b" : 8}, {"a" : 5 , "b" : 7}, {"a" : 5, "b" : 8}]
    >>> order = QueryOrder([("a",True)])
    >>> order(items)
    [{'a': 4, 'b': 8}, {'a': 5, 'b': 7}, {'a': 5, 'b': 8}]
    >>> order = QueryOrder([("a", True), ("b", False)])
    >>> order(items)
    [{'a': 4, 'b': 8}, {'a': 5, 'b': 8}, {'a': 5, 'b': 7}]


    """

    def __init__(self, orderbys):
        self.orderbys = orderbys
    
    def __call__(self, items):
        s = list(items)
        orders = list(self.orderbys)
        orders.reverse()
        for key, order in orders:
            s = sorted(s, key = itemgetter(key), reverse = not order)
        return s


class QuerySelect(Query):
    """Returns a partial view of the items


    >>> items = [{"a": 2, "b": 3}, {"a":4, "b": 5}]
    >>> select = QuerySelect({'label': 'a'})
    >>> list(select(items))
    [{'label': 2}, {'label': 4}]

    """

    def __init__(self, mapping = {}, object_mapping = None):
        if object_mapping is not None:
            self.mapping = object_mapping
        else:
            self.mapping = dict([(name, make_request_property(value)) for name,
                value in mapping.items()])
        self.__mapping = dict(self.mapping)
        self.__classify()


    def __classify(self):
        self.sub_selects = {}
        #dict of dict
        sub_mappings = {}
        for alias, prop in self.mapping.items():
            if prop.child_property is not None:
                sub_mapping = sub_mappings.setdefault(prop.property_name, {})
                sub_mapping[alias] = prop.child_property
                self.__mapping.pop(alias)
        self.sub_selects = dict([(name, QuerySelect(object_mapping = value)) \
            for name, value in sub_mappings.items()])
            

    def __sub_generator(self, prop,item):
        return self.sub_selects[prop](item[prop])
        
    def __transform(self, item):
        return dict([(alias, prop.getValue(item)) \
             for alias, prop in self.__mapping.items()])

    def __call__(self, items):
        if isinstance(items, Item) or isinstance(items, dict):
            items = [items]
        for item in items :
            newitem = self.__transform(item)
            sub_generators = [sub_select(item[prop]) \
                for prop, sub_select in self.sub_selects.items()]
            sub_generators = list(sub_generators)
            if len(sub_generators) == 0 :
                yield newitem
            else :
                for cartesian_item in itertools.product(*sub_generators):
                    for cartesian_atom in cartesian_item:
                        cartesian_atom.update(newitem)
                        yield cartesian_atom 



class QueryRange(Query):
    """ Return a range of items

    >>> items = itertools.cycle([{'a':1, 'b':1},{'a':2, 'b': 2}])
    >>> range = QueryRange(slice(1,3))
    >>> list(range(items))
    [{'a': 2, 'b': 2}, {'a': 1, 'b': 1}]
    

    """

    def __init__(self, range):
        self.range = range

    def __call__(self, items):
        range_iter = itertools.islice(items, self.range.start, self.range.stop)
        for item in range_iter:
            yield item
