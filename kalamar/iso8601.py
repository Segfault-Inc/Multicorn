# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2007 Michael Twomey
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
# along with Dyko.  If not, see <http://www.gnu.org/licenses/>.

"""
ISO 8601 date time string parsing.

Basic usage:
>>> import iso8601
>>> iso8601.parse_date('2007-01-25T12:00:00Z') # doctest:+ELLIPSIS
datetime.datetime(2007, 1, 25, 12, 0, tzinfo=<...iso8601.Utc object at...>)
>>>

"""

from datetime import datetime, timedelta, tzinfo
import re

__all__ = ['parse_date', 'ParseError']

# Adapted from http://delete.me.uk/2005/03/iso8601.html
ISO8601_REGEX = re.compile(
    r'(?P<year>[0-9]{4})(-(?P<month>[0-9]{1,2})(-(?P<day>[0-9]{1,2})'
    r'((?P<separator>.)(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2})'
    r'(:(?P<second>[0-9]{2})(\.(?P<fraction>[0-9]+))?)?'
    r'(?P<timezone>Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?')
TIMEZONE_REGEX = re.compile(
    '(?P<prefix>[+-])(?P<hours>[0-9]{2}).(?P<minutes>[0-9]{2})')

class ParseError(Exception):
    """Exception raised when there is a problem parsing a date string."""
    pass

# Yoinked from python docs
ZERO = timedelta(0)
class Utc(tzinfo):
    """UTC timezone information."""
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return 'UTC'

    def dst(self, dt):
        return ZERO
UTC = Utc()

class FixedOffset(tzinfo):
    """Fixed offset in hours and minutes from UTC."""
    def __init__(self, offset_hours, offset_minutes, name):
        self.__offset = timedelta(hours=offset_hours, minutes=offset_minutes)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO
    
    def __repr__(self):
        return '<FixedOffset %r>' % self.__name

def parse_timezone(tzstring, default_timezone=UTC):
    """Parse ISO 8601 timezone specs into tzinfo offsets."""
    if tzstring == 'Z':
        return default_timezone
    # This isn't strictly correct, but it's common to encounter dates without
    # timezones so I'll assume the default (which defaults to UTC).
    # Addresses issue 4.
    if tzstring is None:
        return default_timezone
    match = TIMEZONE_REGEX.match(tzstring)
    prefix, hours, minutes = match.groups()
    hours, minutes = int(hours), int(minutes)
    if prefix == '-':
        hours, minutes = -hours, -minutes
    return FixedOffset(hours, minutes, tzstring)

def parse_date(datestring, default_timezone=UTC):
    """Parses ISO 8601 dates into datetime objects.
    
    The timezone is parsed from the date string. However it is quite common to
    have dates without a timezone (not strictly correct). In this case the
    default timezone specified in default_timezone is used. This is UTC by
    default.

    """
    if not isinstance(datestring, basestring):
        raise ParseError('Expecting a string %r' % datestring)
    match = ISO8601_REGEX.match(datestring)
    if not match:
        raise ParseError('Unable to parse date string %r' % datestring)
    groups = match.groupdict()
    tz = parse_timezone(groups['timezone'], default_timezone=default_timezone)
    if (groups['hour'], groups['minute'], groups['second']) == 3 * (None,):
        groups['hour'], groups['minute'], groups['second'] = 0, 0, 0
    if groups['fraction'] is None:
        groups['fraction'] = 0
    else:
        groups['fraction'] = int(float('0.%s' % groups['fraction']) * 1e6)
    return datetime(
        int(groups['year']), int(groups['month']), int(groups['day']),
        int(groups['hour']), int(groups['minute']), int(groups['second']),
        int(groups['fraction']), tz)
