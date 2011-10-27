from . import ForeignDataWrapper
from .utils import log_to_postgres, ERROR
from imaplib import IMAP4, IMAP4_SSL, ParseFlags
import email


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
        if self.ssl:
            self.imap_agent = IMAP4_SSL(self.host)
        else:
            self.imap_agent = IMAP4(self.host)
        if self.login:
            self.imap_agent.login(self.login, self.password)
        self.imap_agent.select(self.folder)

    def make_like(self, field_name, value):
        if value.startswith('%'):
            value = value[1:]
        if value.endswith('%'):
            value = value[:-1]
        if '%' not in value:
            return '%s "%s"' % (field_name.upper(), value)
        return ''

    def execute(self, quals, columns):
        condition = ''
        log_to_postgres(str(quals))
        conditions = []
        for qual in quals:
            if qual.field_name == self.payload_column:
                # Its different
                if qual.operator == '=':
                    conditions.append('TEXT "%s"' % qual.value)
                elif qual.operator == '~~':
                    conditions.append(self.make_like("TEXT", qual.value))
            elif qual.field_name == self.flags_column:
                pass
            elif qual.operator == '~~':
                conditions.append(self.make_like(qual.field_name, qual.value))
            elif qual.operator == '!~~':
                conditions.append('NOT %s ' % (self.make_like(qual.field_name, qual.value)))
            elif qual.operator == '!=':
                conditions.append('NOT %s "%s"' % (qual.field_name.upper(), qual.value))
            elif qual.operator == '=':
                conditions.append('%s "%s"' % (qual.field_name.upper(), qual.value))
        condition = '(%s)' % ' '.join(('(%s)' % cond for cond in conditions)) if conditions else 'ALL'
        headers = []
        need_flags = False
        need_text = False
        for column in list(columns):
            if column == self.payload_column:
                need_text = True
            elif column == self.flags_column:
                need_flags = True
            else:
                headers.append(column)
        fetch_string = "(%s%s%s)" % (
                'BODY[TEXT] ' if need_text else '',
                ('BODY[HEADER.fields (%s)]' % (' '.join(headers)))
                    if headers else '',
                ' FLAGS' if need_flags else ''
                )
        matching_mails = ','.join(self.imap_agent.search("UTF8", condition)[1][0].split())
        if matching_mails:
            typ, data = self.imap_agent.fetch(matching_mails, fetch_string)
            data = iter(data)
            item = {}
            for raw in data:
                if raw == ')':
                    # End of an item
                    yield item
                    item = {}
                elif isinstance(raw, tuple):
                    mail = email.message_from_string(raw[1])
                    if 'BODY[TEXT]' in raw[0]:
                        # Payload
                        item[self.payload_column] = mail.get_payload(
                                decode=True)
                    if 'FLAGS' in raw[0]:
                        item[self.flags_column] = ParseFlags(raw[0])
                    if 'BODY[HEADER.FIELDS' in raw[0]:
                        # Its the headers
                        for header in headers:
                            item[header] = mail.get(header)

