# coding: utf8

from __future__ import division

import itertools
import functools
from expressions import r, evaluate
import aggregates as a


class _Query(object):
    def __init__(self, parts=None):
        self.parts = parts or ()
    
    def _add_part(self, *new_part):
        new_parts = self.parts + (new_part,)
        return _Query(new_parts)
    
    def select(self, **new_data):
        return self._add_part('select', new_data)
    
    def where(self, condition):
        return self._add_part('where', condition)
        
    def sort(self, *keys):
        return self._add_part('sort', keys)
    
    def aggregate(self, by=None, **aggregated_data):
        return self._add_part('aggregate', by or a.By(), aggregated_data)
    
    def __getitem__(self, index):
        if not isinstance(index, slice):
            raise ValueError('Query objects only support slicing, not indexing')
        return self._add_part('slice', index)

Query = _Query()


def execute_select(data, expressions):
    for element in data:
        yield dict((name, evaluate(expression, element))
                   for name, expression in expressions.iteritems())

def execute_where(data, condition):
    return itertools.ifilter(functools.partial(evaluate, condition), data)

def execute_sort(data, key_expressions):
    def key_function(element):
        return tuple(evaluate(key, element) for key in key_expressions)
    return sorted(data, key=key_function)

def execute_aggregate(data, by, aggregates):
    criteria = by.criteria
    names = sorted(criteria.keys())
    groups = {}
    for element in data:
        values = tuple(evaluate(criteria[name], element) for name in names)
        groups.setdefault(values, []).append(element)
        # TODO: maybe do not keep all elements but compute aggregates
        # step-by-step?
        
    for criteria_values, elements in groups.iteritems():
        result = dict(zip(names, criteria_values))
        for name, aggregate in aggregates.iteritems():
            aggregated_values = (evaluate(aggregate.expression, element)
                                 for element in elements)
            if isinstance(aggregate, a.Count):
                value = len(elements)
                # aggregated_values not used
            elif isinstance(aggregate, a.Sum):
                value = sum(aggregated_values)
            elif isinstance(aggregate, a.Avg):
                value = sum(aggregated_values) / len(elements)
            elif isinstance(aggregate, a.Min):
                value = min(aggregated_values)
            elif isinstance(aggregate, a.Max):
                value = max(aggregated_values)
            else:
                raise ValueError('Unkown aggregate type: ', aggregate)
            
            result[name] = value
        yield result
        

def execute_slice(data, slice_):
    stops = (slice_.start, slice_.stop, slice_.step)
    if all(i is None or i >= 0 for i in stops):
        return itertools.islice(data, *stops)
    else:
        # Negatives values for start, end or step imply to know where
        # the end of the sequence is (which islice doesn’t),
        # so we build a list
        return list(data)[slice_]


EXECUTORS = {
    'select': execute_select,
    'where': execute_where,
    'sort': execute_sort,
    'aggregate': execute_aggregate,
    'slice': execute_slice,
}

def execute(query, data):
    """`data`: an iterable of mappings."""
    for part in query.parts:
        data = EXECUTORS[part[0]](data, *part[1:])
    return data


