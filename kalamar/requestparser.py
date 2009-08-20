#!/usr/bin/env python

import re
from kalamar import utils
from kalamar.utils import Condition
from kalamar import iso8601
#from time import sleep

def iparse(data):
    p = Parser(data)
    return  p.iparse()

def parse(data):
    """
    >>> parse('')
    []
    
    """
    return list(iparse(data))

class RequestSyntaxError(ValueError): pass

class Parser(object):
    ur"""
    ==============
    Empty request:
    ==============
    
    >>> p = Parser(u'')
    >>> p.parse()
    []
    
    =================
    Implicit request:
    =================
    -----------
    None value:
    -----------
    
    >>> p = Parser(u'None')
    >>> p.parse()
    [Condition(None, None, None)]
    
    --------
    Numeric:
    --------
    
    >>> p = Parser(u'1')
    >>> p.parse()
    [Condition(None, None, 1)]
    
    >>> p = Parser(u'077')
    >>> p.parse()
    [Condition(None, None, 63)]

    >>> p = Parser(u'0XF')
    >>> p.parse()
    [Condition(None, None, 15)]
    
    >>> p = Parser(u'0xf')
    >>> p.parse()
    [Condition(None, None, 15)]

    >>> p = Parser(u'/1/')
    >>> p.parse()
    [Condition(None, None, 1)]
    
    >>> p = Parser(u'//1//')
    >>> p.parse()
    [Condition(None, None, 1)]

    >>> p = Parser(ur'1//2//3')
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, 1),
     Condition(None, None, 2),
     Condition(None, None, 3)]
        
    >>> p = Parser(ur'11/22/33')
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, 11),
     Condition(None, None, 22),
     Condition(None, None, 33)]
        
    >>> p = Parser(ur'''
    ... /1
    ... /2
    ... ''')
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, 1),
     Condition(None, None, 2)]
    
    --------
    Strings:
    --------

    >>> p = Parser(ur'''"aa"/'bb'/"cc"''')
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, u'aa'),
     Condition(None, None, u'bb'),
     Condition(None, None, u'cc')]

    >>> p = Parser(ur'''"a"/'b'/"c"''')
    >>> p.parse()
    ... # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, u'a'),
     Condition(None, None, u'b'),
     Condition(None, None, u'c')]
        
    >>> p = Parser(ur"'a/a'/'b/b'")
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, u'a/a'),
     Condition(None, None, u'b/b')]
    
    ----------------
    String escaping:
    ----------------
    
    >>> p = Parser(ur"'a\'a'/'b\\b'")
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, u"a'a"),
     Condition(None, None, u'b\\b')]
    
    Only characters in Parser.escaped_chars are escaped
    >>> p = Parser(ur"'a\a'")
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE
    [Condition(None, None, u'a\\a')]
    
    ----------------
    Date:
    ----------------
    
    >>> p = Parser(ur"1988-02-03")
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [Condition(None, None, datetime.datetime(1988, 2, 3, 0, 0,
    tzinfo=<kalamar.iso8601.Utc object at 0x...>))]
     
    >>> p = Parser(ur"1988-02-03T17:31:15")
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [Condition(None, None, datetime.datetime(1988, 2, 3, 17, 31, 15,
    tzinfo=<kalamar.iso8601.Utc object at 0x...>))]
    
    ================
    Explicit syntax:
    ================
        
    >>> p = Parser(ur"a=1")
    >>> p.parse()
    [Condition(u'a', <built-in function eq>, 1)]

    >>> p = Parser(ur"a>=1")
    >>> p.parse()
    [Condition(u'a', <built-in function ge>, 1)]

    >>> p = Parser(ur"'a'='b'")
    >>> p.parse()
    [Condition(u'a', <built-in function eq>, u'b')]

    >>> p = Parser(ur"'a'>='b'")
    >>> p.parse()
    [Condition(u'a', <built-in function ge>, u'b')]

    >>> p = Parser(ur"a>=1/c~!='d'")
    >>> p.parse() # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    [Condition(u'a', <built-in function ge>, 1),
     Condition(u'c', <function re_not_match at 0x...>, u'd')]

    ===============
    Parsing errors:
    ===============

    >>> p = Parser(u"a a")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Got u'a' but expected operator or u'/'.

    >>> p = Parser(u"'a''")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Got u"'" but expected operator or u'/'.

    >>> p = Parser(u"''a'")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Got u'a' but expected operator or u'/'.

    >>> p = Parser(u"'a\\")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Reached EOF while scanning a quoted string.
    
    >>> p = Parser(ur"'a\'")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Reached EOF while scanning a quoted string.
    
    >>> p = Parser(u"'a")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Reached EOF while scanning a quoted string.
    
    >>> p = Parser(ur"$")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Character u'$' is not allowed outside quotes.
    
    >>> p = Parser(ur"a$")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Character u'$' is not allowed outside quotes.
    
    >>> p = Parser(ur"a=c=")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Got u'=' but expected u'/'.
    
    >>> p = Parser(ur"a='c'=")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Got u'=' but expected u'/'.
    
    >>> p = Parser(ur"a=")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Got EOF but expected a value.
    
    >>> p = Parser(ur"a==a")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Invalid operator u'=='.
    
    >>> p = Parser(ur"1a")
    >>> p.parse()
    Traceback (most recent call last):
        ...
    RequestSyntaxError: Failed to convert value: u'1a'.


    """
    
    quote_chars = u'\'"'
    escape_char = u'\\'
    escaped_chars = u'\'"\\'
    break_char = u'/'

    @classmethod
    def escape_string(cls, value):
        r"""
        >>> Parser.escape_string(u'Foo "bar"...')
        u'"Foo \\"bar\\"..."'
        """
        # escape the backslash itself first
        value = value.replace(cls.escape_char, cls.escape_char * 2)
        # then all other 
        for char in cls.escaped_chars:
            if char != cls.escape_char:
                value = value.replace(char, cls.escape_char + char)
        return u'"%s"' % value
    
    def __init__(self, data):
        self.data = unicode(data)
        
        self.strings = ['']
        
        self._quote_char = u''
    
    def parse(self):
        return list(self.iparse())

    def iparse(self):
        self._idata = iter(self.data)
        
        try:
            c = self._idata.next()
            self._stop_parsing = False
        except StopIteration:
            self._stop_parsing = True
        
        while not self._stop_parsing:
            self._stop_condition = False
            self._mode = 'begin'
            self._quoted = False
            self._start = True
            self._finish = False
            while not self._stop_condition and not self._stop_parsing:
#                sleep(0.2)
#                print "mode =", self._mode, "/ quoted =", self._quoted, \
#                      "/ start =", self._start, "/finish =", self._finish, \
#                      "/ c ='", c, "'"
                if c == self.escape_char and self._quoted and \
                   (self._mode == 'begin' or self._mode == 'value'):
                    # same behaviour as python's strings escaping
                    try:
                        old_c, c = c, self._idata.next()
                    except StopIteration:
                        raise RequestSyntaxError(
                            "Reached EOF while scanning a quoted string."
                        )
                    if c in self.escaped_chars:
                        self.strings[-1] += c
                        try:
                            c = self._idata.next()
                        except StopIteration:
                            raise RequestSyntaxError(
                                "Reached EOF while scanning a quoted string."
                            )
                    else:
                        self.strings[-1] += old_c
                elif self._mode == 'begin' or self._mode == 'value':
                    if self._start:
                        self._start = False
                        if c in self.quote_chars:
                            self._quoted = True
                            self._quote_char = c
                        elif c == self.break_char or self.is_blank(c):
                            # ignore beginning slashes and blanks
                            self._start = True
                        else:
                            self._quoted = False
                            if self.is_ok(c):
                                self.strings[-1] += c
                            else:
                                raise RequestSyntaxError(
                                    "Character %s is not allowed outside quotes."
                                    % repr(c)
                                )
                        try:
                            c = self._idata.next()
                        except StopIteration:
                                self._stop_parsing = True
                    elif self._finish:
                        if self.is_blank(c):
                            # ignore trailing blanks
                            pass
                        elif c == self.break_char:
                            self._stop_condition = True
                        else:
                            raise RequestSyntaxError(
                                "Got %s but expected operator or %s."
                                % (repr(c), repr(self.break_char))
                            )
                        try:
                            c = self._idata.next()
                        except StopIteration:
                            self._stop_parsing = True
                    else:
                        if self._quoted:
                            if c == self._quote_char:
                                try:
                                    c = self._idata.next()
                                    if self.is_operator_char(c):
                                        if self._mode == 'begin':
                                            self._start = True
                                            self._finish = False
                                            self._mode = 'operator'
                                            self.strings.append('')
                                        else:
                                            raise RequestSyntaxError(
                                                "Got %s but expected %s."
                                                % (repr(c), repr(self.break_char))
                                            )
                                    elif c == self.break_char:
                                        self._stop_condition = True
                                    else:
                                        raise RequestSyntaxError(
                                            "Got %s but expected operator or %s."
                                            % (repr(c), repr(self.break_char))
                                        )
                                except StopIteration:
                                    self._stop_parsing = True
                            elif self.is_ok(c):
                                self.strings[-1] += c
                                try:
                                    c = self._idata.next()
                                except StopIteration:
                                    raise RequestSyntaxError(
                                        "Reached EOF while scanning a quoted string."
                                    )
                            else:
                                raise RequestSyntaxError(
                                    "Character %s is not allowed inside quotes."
                                    % repr(c)
                                )
                        else: # not qoted
                            if c == self.break_char:
                                self._stop_condition = True
                            elif self.is_blank(c):
                                self._finish = True
                            elif self.is_operator_char(c):
                                if self._mode == 'begin':
                                    self._start = True
                                    self._finish = False
                                    self._mode = 'operator'
                                    self.strings.append('')
                                else:
                                    raise RequestSyntaxError(
                                        "Got %s but expected %s."
                                        % (repr(c), repr(self.break_char))
                                    )
                            elif self.is_ok(c):
                                self.strings[-1] += c
                                try:
                                    c = self._idata.next()
                                except StopIteration:
                                    self._stop_parsing = True
                            else:
                                raise RequestSyntaxError(
                                    "Character %s is not allowed outside quotes."
                                    % repr(c)
                                )
                    
                elif self._mode == 'operator':
                    if self.is_operator_char(c):
                        self.strings[-1] += c
                        try:
                            c = self._idata.next()
                        except StopIteration:
                            raise RequestSyntaxError("Got EOF but expected a value.")
                    else:
                        if self.is_operator_seq(self.strings[-1]):
                            self._mode = 'value'
                            self.strings.append('')
                        else:
                            raise RequestSyntaxError(
                                "Invalid operator %s." % repr(self.strings[-1])
                            )
                
            if len(self.strings[-1]) > 0:
                value = self.convert_value(self.strings[-1])
                if self._mode == 'begin':
                    yield Condition(None, None, value)
                elif self._mode == 'value':
                    operator = self.get_operator(self.strings[-2])
                    yield Condition(self.strings[-3], operator, value)
                self.strings = ['']
        
    def is_ok(self, c):
        if self._quoted and (self._mode == 'begin' or self._mode == 'value'):
            return True
        elif self._mode == 'begin' or self._mode == 'value':
            if re.match(ur'^[\w:-]$', c, re.UNICODE):
                return True
        return False
    
    def is_blank(self, c):
        if re.match(ur'^\s$', c, re.UNICODE):
                return True
        return False
    
    def is_operator_seq(self, s):
        return s in utils.operators
    
    def get_operator(self, s):
        return utils.operators[s]
    
    def is_operator_char(self, c):
        return c in "=<>!~"
    
    def convert_value(self, value):
        if self._quoted:
            new_value = value
        elif self.strings[-1] == 'None':
            new_value = None
        elif re.match(r"^0x[abcdef\d]+$|^[\d]+$|^\d+\.?\d+$", value, re.IGNORECASE):
            new_value = eval(value)
        elif iso8601.ISO8601_REGEX.match(value):
            new_value = iso8601.parse_date(value)
        else:
            raise RequestSyntaxError("Failed to convert value: %s." % repr(value))
        
        return new_value

#if __name__ == '__main__':
#    p = Parser()
#    a = p.parse(u" aa ")
#    print a
