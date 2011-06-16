from sqlalchemy import Unicode, Integer, DateTime, Date, Boolean, Numeric

from datetime import date, datetime
from decimal import Decimal


def get_dialect(engine):
    # Todo: manage ACTUAL dialects!
    return BaseDialect()


BASE_TYPES_MAPPING = {
    unicode: Unicode,
    bytes: Unicode,
    unicode: Unicode,
    bytes: Unicode,
    int: Integer,
    datetime: DateTime,
    date: Date,
    bool: Boolean,
    Decimal: Numeric
}


class BaseDialect(object):

    def alchemy_type(self, proptype):
        """Override this for specific types (example: Array of strings in PGSQL
        """
        alchemy_type = BASE_TYPES_MAPPING.get(proptype.type, None)
        if alchemy_type is None:
            raise NotImplementedError(
                    "Dialect %s does not support the type %s" %
                    (type(self), proptype.type))
        return alchemy_type
