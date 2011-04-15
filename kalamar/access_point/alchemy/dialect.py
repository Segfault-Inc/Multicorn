from ... import func
from ... request import RequestProperty
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.sql import functions as sqlfunctions


def get_dialect(engine):
    if PGDialect in engine.__class__.__bases__:
        return PostGresDialect()
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


class PostGresDialect(object):

    SUPPORTED_OPERATORS = AlchemyDialect.SUPPORTED_OPERATORS + ["~=", "~!="]

    def __init__(self):
        self.SUPPORTED_FUNCS.update({
            func.slice:lambda col, func:
                sqlfunctions.substr(col, slice.start, slice.stop - slice.start)})
