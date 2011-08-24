# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from sqlalchemy import Unicode, Integer, DateTime, Date, Boolean, Numeric

from datetime import date, datetime
from decimal import Decimal

from ..wrappers import AlchemyWrapper
from .postgres import PostgresWrapper
from ....requests import types


def get_dialect(engine):
    # Todo: manage ACTUAL dialects!
    if engine.name == 'postgresql':
        return PostgresDialect()
    return BaseDialect()


BASE_TYPES_MAPPING = {
    unicode: Unicode,
    bytes: Unicode,
    int: Integer,
    datetime: DateTime,
    date: Date,
    bool: Boolean,
    Decimal: Numeric
}


class BaseDialect(object):

    RequestWrapper = AlchemyWrapper

    def alchemy_type(self, proptype):
        """Override this for specific types (example: Array of strings in PGSQL
        """
        alchemy_type = BASE_TYPES_MAPPING.get(proptype.type, None)
        if alchemy_type is None:
            raise NotImplementedError(
                    "Dialect %s does not support the type %s" %
                    (type(self), proptype.type))
        return alchemy_type

    def wrap_request(self, request):
        return self.RequestWrapper.from_request(request)

    def _transform_result(self, result, return_type, corn):
        def process_list(result):
            for item in result:
                yield self._transform_result(item, return_type.inner_type, corn)
        if isinstance(return_type, types.List):
            return process_list(result)
        elif return_type.type == dict:
            if return_type == corn.type:
                result = dict(((key, value) for key, value in dict(result).iteritems()
                    if key in corn.properties))
                return corn.create(dict(result))
            elif return_type.corn:
                return return_type.corn.create(dict(result))
            else:
                newdict = {}
                for key, type in return_type.mapping.iteritems():
                    # Even for dicts, sql returns results "inline"
                    if isinstance(type, types.Dict):
                        subresult = {}
                        for subkey in result.keys():
                            subresult[subkey.replace('__%s_' % key,'').strip('__')] = result[subkey]
                        newdict[key] = self._transform_result(subresult, type, corn)
                    elif isinstance(type, types.List):
                        newdict[key] = self._transform_result(result[key][0], type, corn)
                    else:
                        newdict[key] = result[key]
                return newdict
        else:
            result = list(result)
            if len(result) > 1:
                raise ValueError('More than one element in .one()')
            if len(result) == 0:
                raise ValueError('.one() on an empty sequence')
            return result[0]



class PostgresDialect(BaseDialect):

    RequestWrapper = PostgresWrapper

    def _transform_result(self, result, return_type, corn):
        def process_list(result):
            for item in result:
                yield self._transform_result(item, return_type.inner_type, corn)
        if isinstance(return_type, types.List):
            return process_list(result)
        elif return_type.type == dict:
            newdict = {}
            if return_type.corn:
                ordered_dict = sorted(((x, y.type)
                        for x, y in return_type.corn.definitions.iteritems()),
                        key=lambda x: x[0])
            else:
                ordered_dict = sorted(return_type.mapping.iteritems(), key=lambda x: x[0])
            for idx, (key, type) in enumerate(ordered_dict):
                # Even for dicts, sql returns results "inline"
                newdict[key] = self._transform_result(result[idx], type, corn)
            if return_type.corn:
                return return_type.corn.create(newdict)
            else:
                return newdict
        else:
            if hasattr(result, '__iter__'):
                result = list(result)
                if len(result) > 1:
                    raise ValueError('More than one element in .one()')
                if len(result) == 0:
                    raise ValueError('.one() on an empty sequence')
                return result[0]
            else:
                if result is not None and not isinstance(
                        result, return_type.type):
                    return return_type.type(result)
                else:
                    return result

    def wrap_request(self, request):
        return self.RequestWrapper.from_request(request)

