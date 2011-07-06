from . import AbstractCornExtenser

from pprint import isrecursive, saferepr

from ...requests.types import Type, List, Dict
from ...requests.helpers import inject_context, collect
from ...requests import requests
from ...requests import CONTEXT as c
from ...requests.wrappers import RequestWrapper, AttributeWrapper, FilterWrapper, FilterWrapper
from ...python_executor import execute

class ComputedType(Type):

    def __init__(self, type, name=None, corn=None, expression=None, reverse=None):
        super(ComputedType, self).__init__(type, name=name, corn=corn)
        self.expression = expression
        self.reverse = reverse

    def __getattr__(self, key):
        return getattr(self.type, key)

    def __hash__(self):
        return hash((self.expression, saferepr(self)))

    def __eq__(self, other):
        if isinstance(other, ComputedType):
            return self.expression == other.expression and\
                    self.type == other.type
        return False

class ComputedExtenser(AbstractCornExtenser):

    def __init__(self, name, wrapped_corn):
        super(ComputedExtenser, self).__init__(name, wrapped_corn)
        self.computed_properties = {}

    def register(self, name, expression, reverse=None):
        wrapped_expr = RequestWrapper.from_request(expression)
        type = wrapped_expr.return_type((self.wrapped_corn.type,))
        if reverse is None:
            reverse = {}
        self.computed_properties[name] = ComputedType(type, name, self, expression, reverse)
        self.properties[name] = ComputedType(type, name, self, expression)

    def _all(self):
        for item in self.wrapped_corn._all():
            yield self._transform_item(item)

    def execute(self, request):
        # TODO: transform the request!
        wrapped_request = RequestWrapper.from_request(request)
        types = wrapped_request.used_types()
        replacements = {}
        return_type = wrapped_request.return_type()
        chain = requests.as_chain(request)
        # We cannot do this on the all method itself!
        if len(chain) > 1:
            dict_expr = dict((key, p.expression) for key, p in
                self.computed_properties.iteritems() if p in types)
            replacements[chain[0]] = self.wrapped_corn.all.map(c + dict_expr)
        for type, request_parts in types.iteritems():
            if type.corn is self:
                # We should replace every occurence of this request
                # with its transformation
                for request in request_parts:
                    if isinstance(request, AttributeWrapper):
                        replacements[request.wrapped_request] = type.expression
        request = wrapped_request._copy_replace(replacements)
        return self._transform_result(self.wrapped_corn.execute(request),
                return_type)

    def _make_lazy(self, computed):
        def lazy_loader(item):
            expr = computed.expression
            if not isinstance(expr, requests.Request) and hasattr(expr, '__call__'):
                expr = expr(self)
            expr = inject_context(expr, (item,))
            return execute(expr, (item,))
        return lazy_loader

    def _transform_result(self, result, return_type):
        def process_list(result, return_type):
            for item in result:
                yield self._transform_result(item, return_type.inner_type)
        if isinstance(return_type, List):
            return process_list(result, return_type)
        elif return_type == self.type:
            if result is None:
                return result
            return self._transform_item(result)
        elif isinstance(return_type, Dict):
            newdict = {}
            for key, type in return_type.mapping.iteritems():
                newdict[key] = self._transform_result(result[key], type)
            return newdict
        else:
            return result

    def _transform_items(self, items):
        for item in items:
            wrapped_item = dict((key, value) for key, value in item.iteritems()
                    if key not in self.computed_properties)
            for name, property in self.computed_properties.iteritems():
                for key, expr in property.reverse.iteritems():
                    if not isinstance(expr, requests.Request) and hasattr(expr, '__call__'):
                        value = expr(item)
                    else:
                        expr = inject_context(expr, (item,))
                        value = execute(expr, (item,))
                    wrapped_item[key] = value
            yield wrapped_item

    def save(self, *args):
        self.wrapped_corn.save(*list(self._transform_items(args)))

    def _transform_item(self, item):
        base_dict = dict(item)
        base_lazy = {}
        for type in self.computed_properties.values():
            base_dict.pop(type.name, None)
            base_lazy[type.name] = self._make_lazy(type)
        return self.create(base_dict, base_lazy)
