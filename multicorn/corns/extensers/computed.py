from . import AbstractCornExtenser

from pprint import isrecursive, saferepr

from ...item import BaseItem
from ...requests.types import Type, List, Dict
from ...requests.helpers import inject_context, collect
from ...requests import requests
from ...requests import CONTEXT as c
from ...requests import wrappers
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
        if name in self.wrapped_corn.properties:
            raise KeyError("A property named %s is already registered "
                "in the underlying corn" % name)
        if not isinstance(expression, requests.Request)\
                and hasattr(expression, '__call__'):
            expression = expression(self)
        wrapped_expr = wrappers.RequestWrapper.from_request(expression)
        type = wrapped_expr.return_type((self.wrapped_corn.type,))
        if reverse is None:
            reverse = {}
        self.computed_properties[name] = ComputedType(type, name, self, expression, reverse)
        self.properties[name] = ComputedType(type, name, self, expression)

    def _all(self):
        for item in self.wrapped_corn._all():
            yield self.create(item)

    def _transform_request(self, request, contexts=()):
        wrapped_request = wrappers.RequestWrapper.from_request(request)
        types = wrapped_request.used_types(contexts)
        replacements = {}
        main_chain = requests.as_chain(request)
        # We cannot do this on the all method itself!
        if len(main_chain) > 1:
            replacements = {}
            not_found = set()
            for p in types:
                if isinstance(p, ComputedType):
                    found = False
                    for req in types[p]:
                        chain = requests.as_chain(req)
                        if isinstance(chain[0], requests.StoredItemsRequest) and\
                                chain[0].storage is p.corn:
                            # Modify it!
                            found = True
                            replacements.set_default(chain[0], {})[p.name] = p.expression
                    if not found:
                        not_found.add(p)
            replacements = dict([(key, key.map(c + value)) for key, value in
                replacements.iteritems()])
            for p in list(not_found):
                def predicate(req):
                    req = requests.WithRealAttributes(req)
                    if hasattr(req, 'storage') and req.storage is p.corn:
                        return True
                stored = collect(request, predicate)
                for s in stored:
                    not_found = not_found - set([p])
                    req = replacements.get(s, p.corn.wrapped_corn.all)
                    replacements[s] = req.map(c + {p.name: p.expression})
            request = wrapped_request._copy_replace(replacements)
            if not_found:
                for corn in set(p.corn for p in not_found):
                    request = corn._transform_request(request)
            return request
        return request



    def execute(self, request):
        wrapped_request = wrappers.RequestWrapper.from_request(request)
        return_type = wrapped_request.return_type()
        # TODO: transform the request!
        return self._transform_result(self.wrapped_corn.execute(
            self._transform_request(request)),
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
            return self.create(result)
        elif isinstance(return_type, Dict):
            newdict = {}
            if result is None:
                return None
            for key, type in return_type.mapping.iteritems():
                newdict[key] = self._transform_result(result[key], type)
            return newdict
        else:
            return result

    def _transform_items(self, items):
        for item in items:
            wrapped_item = dict((key, item[key]) for key in item
                    if key not in self.computed_properties)
            for name, property in self.computed_properties.iteritems():
                for key, expr in property.reverse.iteritems():
                    if (not isinstance(expr, requests.Request) and
                        hasattr(expr, '__call__')):
                        value = expr(item)
                    else:
                        expr = inject_context(expr, (item,))
                        value = execute(expr, (item,))
                    wrapped_item[key] = value
            yield wrapped_item

    def save(self, *args):
        transformed_items = list(self._transform_items(args))
        self.wrapped_corn.save(*transformed_items)
        for old_item, new_item in zip(args, transformed_items):
            for key, value in new_item.iteritems():
                if new_item[key] != old_item[key]:
                    old_item[key] = new_item[key]

    def delete(self, item):
        transformed_item = list(self._transform_items([item]))[0]
        self.wrapped_corn.delete(transformed_item)

    def create(self, props=None, lazy_props=None):
        self_props = {}
        if isinstance(props, BaseItem):
           item = props
        else:
            for name in self.computed_properties:
                value = props.pop(name, None)
                if value:
                    self_props[name] = value
            item = self.wrapped_corn.create(props, lazy_props)
        lazy_values = dict(item._lazy_values)
        values = dict(item._values)
        values.update(self_props)
        for type in self.computed_properties.values():
            if type.name not in values:
                lazy_values[type.name] = self._make_lazy(type)
        return super(ComputedExtenser, self).create(values, lazy_values)


class Relation(object):

    def __init__(self, name, to, on, uses, multiple=True, reverse_suffix='s'):
        self.name = name
        self.to = to
        self.on = on
        self.uses = uses
        self.multiple = multiple
        self.reverse_suffix = reverse_suffix

class RelationExtenser(ComputedExtenser):


    def __init__(self, *args, **kwargs):
        super(RelationExtenser, self).__init__(*args, **kwargs)
        self.relations = []
        self._pending_relations = []

    def _bind_relations(self, multicorn):
        for relation in list(self._pending_relations):
            if isinstance(relation.to, basestring):
                if relation.to == self.name:
                    #Auto join, the corn is not yet registered
                    remote_corn = self
                else:
                    remote_corn = multicorn.corns.get(relation.to, None)
                    if remote_corn is None:
                        # If the remote_corn has not been yet registered, skip
                        # this
                        continue
            else:
                remote_corn = relation.to
            relation.to = remote_corn
            if relation.on is None:
                if len(remote_corn.identity_properties) != 1:
                    raise KeyError("Unable relation.to build relationship: remote_corn has more"
                            "than relation.one identity properties")
                relation.on = remote_corn.identity_properties[0]
                # Nothing is given, so the relation is NOT multiple
                relation.multiple = False
            remote_attr = requests.AttributeRequest(subject=c,
                attr_name=relation.on)
            if relation.uses == relation.name:
                raise KeyError("The relation can not be built on a property"
                               "with the same name!")
            if relation.uses is None:
                relation.uses = "%s_%s" % (remote_corn.name, relation.on)
            if relation.uses not in self.wrapped_corn.properties:
                remote_type = remote_corn.properties[relation.on]
                self.wrapped_corn.register(relation.uses, type=remote_type.type)
                self.properties[relation.uses] = self.wrapped_corn.properties[relation.uses]
            self_attr = requests.AttributeRequest(subject=c(-1), attr_name=relation.uses)
            foreign = remote_corn.all.filter(remote_attr == self_attr)
            if not relation.multiple:
                foreign = foreign.one(None)
            # 2nd closure to hold relaiton
            def get_link_getter(relation):
                def link_getter(item):
                    foreign = item[relation.name]
                    if isinstance(foreign, BaseItem):
                        return foreign[relation.on]
                    return foreign
                return link_getter
            reverse = {relation.uses: get_link_getter(relation)}

            self.relations.append(relation)
            self._pending_relations.remove(relation)

            if relation.reverse_suffix:
                if isinstance(remote_corn, ComputedExtenser):
                    ComputedExtenser.register(remote_corn,
                        "%s%s" % (self.name.lower(), relation.reverse_suffix),
                        self.all.filter(
                            getattr(c, relation.uses) == getattr(c(-1), relation.on)))
            super(RelationExtenser, self).register(relation.name, foreign, reverse)

    def bind(self, multicorn):
        self._bind_relations(multicorn)
        super(RelationExtenser, self).bind(multicorn)


    def __replace_by_id_props(self, request, relation):
        replacement = requests.LiteralRequest(True)
        for key in relation.to.identity_properties:
            attr =  requests.AttributeRequest(subject=request.subject.wrapped_request, attr_name=key)
            other = requests.AttributeRequest(subject=request.other.wrapped_request, attr_name=key)
            replacement = requests.AndRequest(replacement, attr == other)
        return replacement


    def register(self, name, to, on=None, uses=None, multiple=True, reverse_suffix='s'):
        """Do not actually register the property, wait for late binding"""
        self._pending_relations.append(Relation(name, to, on, uses, multiple, reverse_suffix))


    def registration(self):
        self._bind_relations(self.multicorn)
