# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
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

from . import item


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


def to_unicode(value):
    """Cast ``value`` into unicode object."""
    return unicode(value)

def to_int(value):
    """Cast ``value`` into int object."""
    return int(value)

def to_float(value):
    """Cast ``value`` into float object."""
    return float(value)

def to_decimal(value):
    """Cast ``value`` into decimal object."""
    return decimal.Decimal(str(value))

def to_datetime(value):
    """Cast ``value`` into datetime object.

    >>> to_datetime("2010-08-04")
    datetime.datetime(2010, 8, 4, 0, 0)
    >>> to_datetime("2010-08-04T20:34:31")
    datetime.datetime(2010, 8, 4, 20, 34, 31)
    >>> to_datetime("2010-08-04T20:34:31Z")
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

    """
    if isinstance(value, datetime.datetime):
        return value
    elif isinstance(value, datetime.date):
        return value.datetime(value.year, value.month, value.day)
    elif isinstance(value, basestring):
        if len(value) == 10:
            return datetime.datetime.strptime(value, "%Y-%m-%d")
        elif len(value) == 19:
            return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        elif len(value) == 20 and value.endswith("Z"):
            value = value[:-1] + "+00:00"
        if len(value) == 25:
            time, timezone = value[:19], value[19:]
            hours, minutes = timezone.split(":")
            time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")
            return time.replace(
                tzinfo=FixedOffsetTimeZone(int(hours), int(minutes)))
    raise ValueError

def to_date(value):
    """Cast ``value`` into date object."""
    if isinstance(value, datetime.date):
        return value
    elif isinstance(value, datetime.datetime):
        return value.date()
    elif isinstance(value, basestring):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError

def to_stream(value):
    for method in ("read", "write", "close"):
        if not hasattr(value, method):
            # value does not look like a stream
            raise ValueError
    return value

def to_iter(value):
    if hasattr(value, "__iter__"):
        return value
    raise ValueError

def to_type(value, data_type):
    """Return ``value`` if instance of ``data_type`` else raise error."""
    if isinstance(value, data_type):
        return value
    raise ValueError('Value %r is not of type %r' % (value, data_type))


PROPERTY_TYPES = {
    unicode: to_unicode,
    int: to_int,
    float: to_float,
    decimal.Decimal: to_decimal,
    io.IOBase: to_stream,
    datetime.datetime: to_datetime,
    datetime.date: to_date,
    iter: to_iter,
    item.Item: lambda value: to_type(value, item.Item)}
