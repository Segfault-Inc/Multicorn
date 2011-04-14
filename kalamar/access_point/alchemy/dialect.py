from ... import func
from ... request import RequestProperty
from sqlalchemy.dialects.postgresql.base import PGDialect



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
        RequestProperty: lambda x: x
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
