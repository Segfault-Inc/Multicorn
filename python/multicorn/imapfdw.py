from . import ForeignDataWrapper, ANY, ALL
from .utils import log_to_postgres, ERROR, WARNING

from imaplib import IMAP4

import re

from email.header import decode_header

from imapclient import IMAPClient

STANDARD_FLAGS = {
        'seen': 'Seen',
        'flagged': 'Flagged',
        'delete': 'Deleted',
        'draft': 'Draft',
        'recent': 'Recent'
}

SEARCH_HEADERS = ['BCC', 'CC', 'FROM', 'TO']


class NoMatchPossible(Exception):
    """An exception raised when the conditions can NOT be met by any message,
    ever."""


def make_or(values):
    """Create an imap OR filter based on a list of conditions to be or'ed"""
    values = filter(lambda x: x not in (None, '()'), values)
    if values:
        if len(values) > 1:
            return reduce(lambda x, y: '(OR %s %s)' % (x, y), values)
        else:
            return values[0]


class ImapFdw(ForeignDataWrapper):
    """An imap foreign data wrapper
    """

    def __init__(self, options, columns):
        super(ImapFdw, self).__init__(options, columns)
        self._imap_agent = None
        self.host = options.get('host', None)
        if self.host is None:
            log_to_postgres('You MUST set the imap host',
            ERROR)
        self.port = options.get('port', None)
        self.ssl = options.get('ssl', False)
        self.login = options.get('login', None)
        self.password = options.get('password', None)
        self.folder = options.get('folder', 'INBOX')
        self.columns = columns
        self.payload_column = options.get('payload_column', None)
        self.flags_column = options.get('flags_column', None)
        self.internaldate_column = options.get('internaldate_column', None)

    def get_rel_size(self, quals, columns):
        """Inform the planner that it can be EXTREMELY costly to use the 
        payload column, and that a query on Message-ID will return only one row."""
        width = len(columns) * 100
        nb_rows = 1000000
        if self.payload_column in columns:
            width += 100000000000
        nb_rows = nb_rows / (10 ** len(quals))
        for qual in quals:
            if qual.field_name.lower() == 'in-reply-to' and\
                    qual.operator == '=':
                nb_rows = 10
            if qual.field_name.lower() == 'message-id' and qual.operator == '=':
                nb_rows = 1
                break
        return (nb_rows, width)

    def _create_agent(self):
        self._imap_agent = IMAPClient(self.host, self.port, ssl=self.ssl)
        if self.login:
            self._imap_agent.login(self.login, self.password)
        self._imap_agent.select_folder(self.folder)

    @property
    def imap_agent(self):
        if self._imap_agent is None:
            self._create_agent()
        try:
            self._imap_agent.select_folder(self.folder)
        except IMAP4.abort:
            self._create_agent()
        return self._imap_agent

    def get_path_keys(self):
        """Helps the planner by supplying a list of list of access keys, as well
        as a row estimate for each one."""
        return [(('Message-ID',), 1), (('From',), 100), (('To',), 100),
                (('In-Reply-To',), 10)]

    def _make_condition(self, key, operator, value):
        if operator not in ('~~', '!~~', '=', '<>', '@>', '&&', '~~*', '!~~*'):
            # Do not manage special operators
            return ''
        if operator in ('~~', '!~~', '~~*', '!~~*') and\
                isinstance(value, basestring):
            # 'Normalize' the sql like wildcards
            if value.startswith(('%', '_')):
                value = value[1:]
            if value.endswith(('%', '_')):
                value = value[:-1]
            if re.match(r'.*[^\\][_%]', value):
                return ''
            value = value.replace('\\%', '%').replace('\\_', '_')
        prefix = ''
        if operator in ('!~~', '<>', '!~~*'):
            if key == self.flags_column:
                prefix = 'UN'
            else:
                prefix = 'NOT '
            if isinstance(value, basestring):
                if value.lower() in STANDARD_FLAGS:
                    prefix = ''
                    value = value.upper()
        if key == self.flags_column:
            if operator == '@>':
                # Contains on flags
                return ' '.join(['%s%s' % (prefix,
                    (STANDARD_FLAGS.get(atom.lower(), '%s %s'
                    % ('KEYWORD', atom))))  for atom in value])
            elif operator == '&&':
                # Overlaps on flags => Or
                values = ['(%s%s)' %
                    (prefix, (STANDARD_FLAGS.get(atom.lower(), '%s %s' %
                    ('KEYWORD', atom))))  for atom in value]
                return make_or(values)
            else:
                value = '\\\\%s' % value
        elif key == self.payload_column:
            value = 'TEXT "%s"' % value
        elif key in SEARCH_HEADERS:
            value = '%s "%s"' % (key, value)
        else:
            # Special case for Message-ID and In-Reply-To:
            # zero-length strings are forbidden so dont bother
            # searching them
            if not value:
                raise NoMatchPossible()
            prefix = 'HEADER '
            value = '%s "%s"' % (key, value)
        return '%s%s' % (prefix, value)

    def extract_conditions(self, quals):
        """Build an imap search criteria string from a list of quals"""
        conditions = []
        for qual in quals:
            # Its a list, so we must translate ANY to OR, and ALL to AND
            if qual.list_any_or_all == ANY:
                values = [
                    '(%s)' % self._make_condition(qual.field_name,
                        qual.operator[0], value)
                    for value in qual.value]
                conditions.append(make_or(values))
            elif qual.list_any_or_all == ALL:
                conditions.extend([
                    self._make_condition(qual.field_name, qual.operator[0],
                        value)
                    for value in qual.value])
            else:
                # its not a list, so everything is fine
                conditions.append(self._make_condition(qual.field_name,
                    qual.operator, qual.value))
        conditions = filter(lambda x: x not in (None, '()'), conditions)
        return conditions

    def execute(self, quals, columns):
        # The header dictionary maps columns to their imap search string
        col_to_imap = {}
        headers = []
        for column in list(columns):
            if column == self.payload_column:
                col_to_imap[column] = 'BODY[TEXT]'
            elif column == self.flags_column:
                col_to_imap[column] = 'FLAGS'
            elif column == self.internaldate_column:
                col_to_imap[column] = 'INTERNALDATE'
            else:
                col_to_imap[column] = 'BODY[HEADER.FIELDS (%s)]' %\
                        column.upper()
                headers.append(column)
        try:
            conditions = self.extract_conditions(quals) or ['ALL']
        except NoMatchPossible:
            matching_mails = []
        else:
            matching_mails = self.imap_agent.search(charset="UTF8",
                criteria=conditions)
        if matching_mails:
            data = self.imap_agent.fetch(matching_mails, col_to_imap.values())
            item = {}
            for msg in data.values():
                for column, key in col_to_imap.iteritems():
                    item[column] = msg[key]
                    if column in headers:
                        item[column] = item[column].split(':', 1)[-1].strip()
                        values = decode_header(item[column])
                        for decoded_header, charset in values:
                            # Values are of the from "Header: value"
                            if charset:
                                try:
                                    item[column] = decoded_header.decode(
                                            charset)
                                except LookupError:
                                    log_to_postgres('Unknown encoding: %s' %
                                            charset, WARNING)
                            else:
                                item[column] = decoded_header
                yield item
