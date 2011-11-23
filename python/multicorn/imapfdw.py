from . import ForeignDataWrapper, ANY, ALL
from .utils import log_to_postgres, ERROR
import time

from email.header import decode_header

from imapclient import IMAPClient

STANDARD_FLAGS = {
        'seen': 'Seen',
        'flagged': 'Flagged',
        'delete': 'Deleted',
        'draft': 'Draft',
        'recent': 'Recent'
}


def make_or(values):
    """Create an imap OR filter based on a list of conditions to be or'ed"""
    return reduce(lambda x, y: '(OR %s %s)' % (x, y), values)


class ImapFdw(ForeignDataWrapper):
    """An imap foreign data wrapper
    """

    def __init__(self, options, columns):
        super(ImapFdw, self).__init__(options, columns)
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
        self.imap_agent = IMAPClient(self.host, self.port, ssl=self.ssl)
        self.internaldate_column = options.get('internaldate_column', None)
        if self.login:
            self.imap_agent.login(self.login, self.password)
        self.imap_agent.select_folder(self.folder)

    def _make_condition(self, key, operator, value):
        if operator not in ('~~', '!~~', '=', '<>', '@>', '&&', '~~*', '!~~*'):
            # Do not manage special operators
            return None
        if operator in ('~~', '!~~', '~~*', '!~~*') and\
                isinstance(value, basestring):
            # 'Normalize' the sql like wildcards
            if value.startswith(('%', '_')):
                value = value[1:]
            if value.endswith(('%', '_')):
                value = value[:-1]
            if '%' in value or '_' in value:
                # If any wildcard remains, we cant do anything
                return None
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
        else:
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
                return make_or(values)
            elif qual.list_any_or_all == ALL:
                conditions.extend([
                    self._make_condition(qual.field_name, qual.operator[0],
                        value)
                    for value in qual.value])
            else:
                # its not a list, so everything is fine
                conditions.append(self._make_condition(qual.field_name,
                    qual.operator, qual.value))
        conditions = filter(lambda x: x is not None, conditions)
        return conditions

    def execute(self, quals, columns):
        conditions = ''
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
        conditions = self.extract_conditions(quals) or 'ALL'
        matching_mails = self.imap_agent.search(charset="UTF8",
            criteria=conditions)
        if matching_mails:
            data = self.imap_agent.fetch(matching_mails, col_to_imap.values())
            item = {}
            for msg in data.values():
                for column, key in col_to_imap.iteritems():
                    item[column] = msg[key]
                    if column in headers:
                        item[column] = item[column].replace('%s:' %
                            column.title(), '', 1).strip()
                        values = decode_header(item[column])
                        for decoded_header, charset in values:
                            # Values are of the from "Header: value"
                            if charset:
                                item[column] = decoded_header.decode(charset)
                            else:
                                item[column] = decoded_header
                yield item
