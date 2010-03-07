# coding: utf8

import os
import email
from unittest import TestCase
from test.kalamar import Site
from test.kalamar.test_site_fs_and_sqlite import TestData

class TestMails(TestCase):
    def setUp(self):
        self.temp_dir = TestData.get_temp_dir()
        self.site = Site(os.path.join(self.temp_dir, 'mails.conf'))

    def test_keys(self):
        mail = self.site.open('mails', ['mail'])
        keys = mail.keys()
        for key in ('From', 'To', 'Subject', 'Date', 'Message-ID', 'id'):
            self.assert_(key in keys)

    def test_properties(self):
        mail = self.site.open('mails', ['mail'])
        for key, value in {
            'From': 'John Doe <jdoe@machine.example>',
            'To': 'Mary Smith <mary@example.net>',
            'Subject': 'Saying Hello',
            'Date': 'Fri, 21 Nov 1997 09:55:06 -0600',
            'Message-ID': '<1234@local.machine.example>'}.items():
            self.assertEquals(mail[key], value)

    def test_serialize(self):
        item = self.site.open('mails', ['mail'])
        file_message = email.message_from_string(open(item.filename).read())
        file_payload = file_message.get_payload(decode=True)
        item_message = email.message_from_string(item.serialize())
        item_payload = item_message.get_payload(decode=True)
        self.assertEquals(file_payload, item_payload)
