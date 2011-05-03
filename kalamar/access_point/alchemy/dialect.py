from ... import func
from ... request import RequestProperty
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.sql import func as sqlfunctions, expression
import operator

def get_dialect(engine):
    if PGDialect in engine.__class__.__bases__:
        return PostGresDialect()
    elif SQLiteDialect in engine.__class__.__bases__:
        return SQLite3Dialect()
    else:
        return AlchemyDialect()

class AlchemyDialect(object):

    SUPPORTED_OPERATORS = [
        '=', '!=', '>', '<', '>=', '<', '<=', 'like'
    ]

    SUPPORTED_FUNCS = {
        RequestProperty: lambda col, fun: col,
        func.coalesce: lambda col, fun:
            sqlfunctions.coalesce(col, fun.replacement),
        func.extract: lambda col, extract:
            expression.extract(extract.field, col),
    }

    SUPPORTED_AGGREGATES = {
        func.count: lambda columns, name, func:
            sqlfunctions.count().label(name),
        func.max: lambda columns, name, func:
            sqlfunctions.max(columns[func.property.name]).label(name),
        func.min: lambda columns, name, func:
            sqlfunctions.min(columns[func.property.name]).label(name),
        func.sum: lambda columns, name, func:
            sqlfunctions.sum(columns[func.property.name]).label(name)
    }

    def _find_fun(self, property, clazz=None):
        """Find the corresponding method for a property"""
        clazz = clazz or property.__class__
        fun = getattr(self, 'func_%s' % clazz.__name__, None)
        return fun

    def func_RequestProperty(self, property, tree):
        return tree.find_table(property)

    def func_ComposedRequestProperty(self, property, tree):
        return tree.find_table(property)

    def func_addition(self, property, tree):
        properties = [self.get_selectable(prop, tree) for prop in property.properties]
        return reduce(operator.__add__, properties)

    def func_product(self, property, tree):
        properties = [self.get_selectable(prop, tree) for prop in property.properties]
        return reduce(operator.__mul__, properties)

    def func_division(self, property, tree):
        properties = [self.get_selectable(prop, tree) for prop in property.properties]
        return reduce(operator.__div__, properties)

    def func_substraction(self, property, tree):
        properties = [self.get_selectable(prop, tree) for prop in property.properties]
        return reduce(operator.__sub__, properties)

    def func_coalesce(self, property, tree):
        return sqlfunctions.coalesce(self.get_selectable(property.property, tree),
                property.replacement)

    def func_constant(self, property, tree):
        return property.constant

    def func_extract(self, property, tree):
        return expression.extract(property.field, self.get_selectable(property.property, tree))

    def get_selectable(self, property, tree):
        return self._find_fun(property)(property, tree)

    def supports(self, property, properties):
        return self._find_fun(property) is not None

    def make_condition(self, column, operator, value):
        if operator in self.SUPPORTED_OPERATORS:
            if operator == '=':
                return column == value
            elif operator == '!=':
                return column == value
            elif operator == 'like':
                return column.like(value)
            else:
                return column.op(operator)(value)


class PostGresDialect(AlchemyDialect):

    SUPPORTED_OPERATORS = AlchemyDialect.SUPPORTED_OPERATORS + ["~=", "~!="]

    def __init__(self):
        self.SUPPORTED_FUNCS.update({
            func.slice: lambda col, slicefun:
                sqlfunctions.substr(col, slicefun.range.start, slicefun.range.stop - slicefun.range.start + 1)})

    def func_slice(self, slicefun, tree):
        return sqlfunctions.substr(self.get_selectable(slicefun.property, tree),
                slicefun.range.start,
                slicefun.range.stop - slicefun.range.start + 1)

    def func_upper(self, property, tree):
        return sqlfunctions.upper(self.get_selectable(property.property, tree))


    def func_lower(self, property, tree):
        return sqlfunctions.lower(self.get_selectable(property.property, tree))



class SQLite3Dialect(AlchemyDialect):

    def func_upper(self, property, tree):
        return sqlfunctions.upper(self.get_selectable(property.property, tree))


    def func_lower(self, property, tree):
        return sqlfunctions.lower(self.get_selectable(property.property, tree))



