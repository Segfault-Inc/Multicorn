# coding: utf8

# This file is part of Dyko
# Copyright (c) 2007 Michael Twomey

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

from kalamar import iso8601
import unittest

class TestISO8601(unittest.TestCase):

    def test_iso8601_regex(self):
        assert iso8601.ISO8601_REGEX.match("2006-10-11T00:14:33Z")

    def test_timezone_regex(self):
        assert iso8601.TIMEZONE_REGEX.match("+01:00")
        assert iso8601.TIMEZONE_REGEX.match("+00:00")
        assert iso8601.TIMEZONE_REGEX.match("+01:20")
        assert iso8601.TIMEZONE_REGEX.match("-01:00")

    def test_parse_date(self):
        d = iso8601.parse_date("2006-10-20T15:34:56Z")
        assert d.year == 2006
        assert d.month == 10
        assert d.day == 20
        assert d.hour == 15
        assert d.minute == 34
        assert d.second == 56
        assert d.tzinfo == iso8601.UTC

    def test_parse_date_fraction(self):
        d = iso8601.parse_date("2006-10-20T15:34:56.123Z")
        assert d.year == 2006
        assert d.month == 10
        assert d.day == 20
        assert d.hour == 15
        assert d.minute == 34
        assert d.second == 56
        assert d.microsecond == 123000
        assert d.tzinfo == iso8601.UTC

    def test_parse_date_fraction_2(self):
        """From bug 6
        
        """
        d = iso8601.parse_date("2007-5-7T11:43:55.328Z'")
        assert d.year == 2007
        assert d.month == 5
        assert d.day == 7
        assert d.hour == 11
        assert d.minute == 43
        assert d.second == 55
        assert d.microsecond == 328000
        assert d.tzinfo == iso8601.UTC

    def test_parse_date_tz(self):
        d = iso8601.parse_date("2006-10-20T15:34:56.123+02:30")
        assert d.year == 2006
        assert d.month == 10
        assert d.day == 20
        assert d.hour == 15
        assert d.minute == 34
        assert d.second == 56
        assert d.microsecond == 123000
        assert d.tzinfo.tzname(None) == "+02:30"
        offset = d.tzinfo.utcoffset(None)
        assert offset.days == 0
        assert offset.seconds == 60 * 60 * 2.5

    def test_parse_invalid_date(self):
        try:
            iso8601.parse_date(None)
        except iso8601.ParseError:
            pass
        else:
            assert 1 == 2

    def test_parse_invalid_date2(self):
        try:
            iso8601.parse_date("23")
        except iso8601.ParseError:
            pass
        else:
            assert 1 == 2

    def test_parse_no_timezone(self):
        """issue 4 - Handle datetime string without timezone
        
        This tests what happens when you parse a date with no timezone. While not
        strictly correct this is quite common. I'll assume UTC for the time zone
        in this case.
        """
        d = iso8601.parse_date("2007-01-01T08:00:00")
        assert d.year == 2007
        assert d.month == 1
        assert d.day == 1
        assert d.hour == 8
        assert d.minute == 0
        assert d.second == 0
        assert d.microsecond == 0
        assert d.tzinfo == iso8601.UTC

    def test_parse_no_timezone_different_default(self):
        tz = iso8601.FixedOffset(2, 0, "test offset")
        d = iso8601.parse_date("2007-01-01T08:00:00", default_timezone=tz)
        assert d.tzinfo == tz

    def test_space_separator(self):
        """Handle a separator other than T
        
        """
        d = iso8601.parse_date("2007-06-23 06:40:34.00Z")
        assert d.year == 2007
        assert d.month == 6
        assert d.day == 23
        assert d.hour == 6
        assert d.minute == 40
        assert d.second == 34
        assert d.microsecond == 0
        assert d.tzinfo == iso8601.UTC
