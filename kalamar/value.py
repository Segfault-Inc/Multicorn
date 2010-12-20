# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Value
=====

Kalamar helpers for value casting.

"""

import decimal
import datetime
import io

from .item import Item, AbstractItem

class Iter(object):
    """Iterable object supporting equality testing."""
    def __init__(self, iterable):
        """Initialize the iterable."""
        self.__iterable = iter(iterable)

    def __iter__(self):
        """Return the iterable."""
        return self.__iterable

    def __hash__(self):
        """Hash of the iterable."""
        return hash(self.__iterable)

    def next(self):
        """Return the next value of the iterable."""
        return next(self.__iterable)

    def __eq__(self, iterable):
        """Test equality of all values in an iterable.

        This function does not consume the iterator.

        """
        return tuple(iter(self)) == tuple(iter(iterable))


class FixedOffsetTimeZone(datetime.tzinfo):
    """Fixed offset in hours and minutes from UTC.

    >>> fixed = FixedOffsetTimeZone(-2, 30)
    >>> dt = datetime.date(2007, 1, 25)
    >>> fixed.utcoffset(dt)
    datetime.timedelta(-1, 81000)
    >>> fixed.tzname(dt)
    'UTC-02:30'
    >>> fixed.dst(dt)
    datetime.timedelta(0)
    
    """
    def __init__(self, offset_hours, offset_minutes):
        """Initialize timezone information with given offsets and name."""
        super(FixedOffsetTimeZone, self).__init__()
        self.__offset = datetime.timedelta(
            hours=offset_hours, minutes=offset_minutes)
        self.__name = "UTC%+03i:%02i" % (offset_hours, offset_minutes)

    def utcoffset(self, _):
        """Return offset of local time from UTC, in minutes east of UTC."""
        return self.__offset

    def tzname(self, _):
        """Return the time zone name as a string."""
        return self.__name

    def dst(self, _):
        """Return daylight saving time adjustment, in minutes east of UTC."""
        return datetime.timedelta(0)


def to_datetime(value):
    """Cast ``value`` into :class:`datetime.datetime` object.

    >>> to_datetime(datetime.date(2010, 8, 4))
    datetime.datetime(2010, 8, 4, 0, 0)
    >>> to_datetime(datetime.datetime(2010, 8, 4, 0, 0))
    datetime.datetime(2010, 8, 4, 0, 0)
    >>> to_datetime("20100804")
    datetime.datetime(2010, 8, 4, 0, 0)
    >>> to_datetime("2010-08-04")
    datetime.datetime(2010, 8, 4, 0, 0)
    >>> to_datetime("2010-08-04T20:34:31")
    datetime.datetime(2010, 8, 4, 20, 34, 31)
    >>> to_datetime("2010-08-04T20:34:31Z")
    ... # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    datetime.datetime(2010, 8, 4, 20, 34, 31,
        tzinfo=<kalamar.value.FixedOffsetTimeZone object at ...>)
    >>> to_datetime("20100804-203431Z")
    ... # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    datetime.datetime(2010, 8, 4, 20, 34, 31,
        tzinfo=<kalamar.value.FixedOffsetTimeZone object at ...>)
    >>> to_datetime("2010-08-04T20:34:31+02:30")
    ... # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    datetime.datetime(2010, 8, 4, 20, 34, 31,
        tzinfo=<kalamar.value.FixedOffsetTimeZone object at ...>)
    >>> to_datetime("2010-08-04T20:34:31+02:30")
    ... # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    datetime.datetime(2010, 8, 4, 20, 34, 31,
        tzinfo=<kalamar.value.FixedOffsetTimeZone object at ...>)
    >>> to_datetime(10) # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ....
    ValueError: 10 cannot be cast to datetime.

    """
    if isinstance(value, datetime.datetime):
        return value
    elif isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    elif isinstance(value, basestring):
        value = value.replace("-", "").replace(":", "").replace("T", "")
        if len(value) == 8:
            return datetime.datetime.strptime(value, "%Y%m%d")
        elif len(value) == 14:
            return datetime.datetime.strptime(value, "%Y%m%d%H%M%S")
        elif len(value) == 15 and value.endswith("Z"):
            value = value[:-1] + "+0000"
        if len(value) == 19:
            time, timezone = value[:14], value[14:]
            hours, minutes = timezone[:2], timezone[2:]
            time = datetime.datetime.strptime(time, "%Y%m%d%H%M%S")
            return time.replace(
                tzinfo=FixedOffsetTimeZone(int(hours), int(minutes)))
    raise ValueError("%s cannot be cast to datetime." % value)


def to_date(value):
    """Cast ``value`` into :class:`datetime.date` object.

    >>> to_date(datetime.date(2010, 8, 4))
    datetime.date(2010, 8, 4)
    >>> to_date(datetime.datetime(2010, 8, 4, 0, 0))
    datetime.date(2010, 8, 4)
    >>> to_date("20100804")
    datetime.date(2010, 8, 4)
    >>> to_date("2010-08-04")
    datetime.date(2010, 8, 4)
    >>> to_date(10) # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ....
    ValueError: 10 cannot be cast to date.

    """
    if isinstance(value, datetime.datetime):
        return value.date()
    elif isinstance(value, datetime.date):
        return value
    elif isinstance(value, basestring):
        value = value.replace("-", "").replace(":", "")
        return datetime.datetime.strptime(value, "%Y%m%d").date()
    raise ValueError("%s cannot be cast to date." % value)


def to_stream(value):
    """Cast ``value`` into stream-like object.

    >>> to_stream(open(__file__)) # doctest: +ELLIPSIS
    <...value.py...>
    >>> to_stream(10) # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ....
    ValueError: 10 cannot be cast to stream.

    """
    for method in ("read", "write", "close"):
        if not hasattr(value, method):
            # value does not look like a stream
            raise ValueError("%s cannot be cast to stream." % value)
    return value


def to_iter(value):
    """Cast ``value`` into iterable object.

    >>> to_iter("spam").next()
    's'
    >>> to_iter(["a", "b", "c"]).next()
    'a'
    >>> to_iter(10) # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ....
    ValueError: 10 cannot be cast to iter.

    """
    try:
        return Iter(value)
    except:
        raise ValueError("%s cannot be cast to iter." % value)


def to_type(value, data_type):
    """Return ``value`` if instance of ``data_type`` else raise error.

    >>> to_type(1, int)
    1
    >>> to_type("eggs", unicode)
    'eggs'
    >>> to_type("1+j", complex)
    (1+1j)
    >>> to_type("eggs", float) # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ....
    ValueError: eggs cannot be cast to float.

    """
    if isinstance(value, data_type) or value is None:
        return value
    else:
        try:
            return data_type(value)
        except:
            raise ValueError("%s cannot be cast to %s." % (
                    value, data_type.__name__))


PROPERTY_TYPES = {
    unicode: unicode,
    int: int,
    float: float,
    decimal.Decimal: lambda value: decimal.Decimal(str(value)),
    io.IOBase: to_stream,
    datetime.datetime: to_datetime,
    datetime.date: to_date,
    iter: to_iter,
    bool: bool,
    Item: lambda value: to_type(value, AbstractItem)}
