from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR
import time

from imapclient import IMAPClient


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

    def make_like(self, field_name, value):
        """Build an imap search criteria corresponding to an sql like"""
        if value.startswith('%'):
            value = value[1:]
        if value.endswith('%'):
            value = value[:-1]
        if '%' not in value:
            return '%s "%s"' % (field_name.upper(), value)
        return ''

    def extract_conditions(self, quals):
        """Build an imap search criteria string from a list of quals"""
        conditions = []
        for qual in quals:
            if qual.field_name == self.payload_column:
                # Its different
                if qual.operator == '=':
                    conditions.append('TEXT "%s"' % qual.value)
                elif qual.operator == '~~':
                    conditions.append(self.make_like("TEXT", qual.value))
            elif qual.field_name == self.flags_column:
                if qual.operator == 'CONTAINS':
                    conditions.append('KEYWORD %s' % qual.value)
                elif qual.operator == 'NOT CONTAINS':
                    conditions.append('UNKEYWORD %s' % qual.value)
                elif qual.operator == '=':
                    for value in qual.value:
                        conditions.append('KEYWORD %s' % value)
            elif qual.operator == '~~':
                conditions.append(self.make_like(qual.field_name,
                  qual.value))
            elif qual.operator == '!~~':
                conditions.append('NOT %s ' % (self.make_like(
                  qual.field_name, qual.value)))
            elif qual.operator == '!=':
                conditions.append('NOT %s "%s"' %
                        (qual.field_name.upper(), qual.value))
            elif qual.operator == '=':
                conditions.append('%s "%s"' %
                        (qual.field_name.upper(), qual.value))
        if conditions:
            condition = '(%s)' % ' '.join(('(%s)' % cond
                for cond in conditions))
        else:
            condition = 'ALL'
        return condition

    def execute(self, quals, columns):
        condition = ''
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
        condition = self.extract_conditions(quals)
        matching_mails = self.imap_agent.search(charset="UTF8",
            criteria=condition)
        if matching_mails:
            data = self.imap_agent.fetch(matching_mails, col_to_imap.values())
            item = {}
            for msg in data.values():
                for column, key in col_to_imap.iteritems():
                    item[column] = msg[key]
                    if column in headers:
                        # Values are of the from "Header: value"
                        item[column] = item[column].replace('%s:' %
                            column.title(), '', 1)
                yield item
