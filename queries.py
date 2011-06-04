# coding: utf8

from __future__ import division

import itertools
import functools
from expressions import r, evaluate
import aggregates as a


class _Query(object):
    def __init__(self, operations=None):
        self.operations = operations or ()

    def _add_operation(self, operation, *args):
        return _Query(self.operations + ((operation, args),))

    def select(self, **data):
        return self._add_operation('select', data)

    def select_also(self, **new_data):
        return self._add_operation('select_also', new_data)

    def where(self, condition):
        return self._add_operation('where', condition)

    def sort(self, *keys):
        return self._add_operation('sort', keys)

    def aggregate(self, by=None, **aggregated_data):
        return self._add_operation('aggregate', by or a.By(), aggregated_data)

    def __getitem__(self, index):
        if not isinstance(index, slice):
            raise ValueError('Query objects only support slicing, not indexing')
        return self._add_operation('slice', index)

Query = _Query()


def _evaluate_dict(element, expressions):
    """
    {name: expression} => {name: evaluated_value}
    """
    for name, expression in expressions.iteritems():
        yield name, evaluate(expression, element)

class PythonExecutor(object):
    @staticmethod
    def execute_select(data, expressions):
        for element in data:
            yield dict(_evaluate_dict(element, expressions))

    @staticmethod
    def execute_select_also(data, expressions):
        for element in data:
            # Merge existing values and new, evaluated values.
            # New values with the same name can override existing ones.
            new_element = dict(element)
            new_element.update(_evaluate_dict(element, expressions))
            yield new_element

    @staticmethod
    def execute_where(data, condition):
        for element in data:
            if evaluate(condition, element):
                yield element

    @staticmethod
    def execute_sort(data, key_expressions):
        def key_function(element):
            return tuple(evaluate(key, element) for key in key_expressions)
        return sorted(data, key=key_function)

    @staticmethod
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


    @staticmethod
    def execute_slice(data, slice_):
        stops = (slice_.start, slice_.stop, slice_.step)
        if all(i is None or i >= 0 for i in stops):
            return itertools.islice(data, *stops)
        else:
            # Negatives values for start, end or step imply to know where
            # the end of the sequence is (which islice doesn’t),
            # so we build a list
            return list(data)[slice_]


def execute_operation(data, operation):
    name, args = operation
    return getattr(PythonExecutor, 'execute_' + name)(data, *args)

def execute(data, query):
    """`data`: an iterable of mappings."""
    return reduce(execute_operation, query.operations, data)


