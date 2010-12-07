from ..test_combinations import first_ap, second_ap
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.unicode_stream import UnicodeStream

import tempfile
import shutil


def teardown_fs(access_point):
    shutil.rmtree(access_point.wrapped_ap.root_dir)

@first_ap(teardown=teardown_fs)
def first_ap_fs():
    temp_dir = tempfile.mkdtemp()
    fs_ap = FileSystem(temp_dir, "(.*)/(.*)/(.*)",(
        ('id', FileSystemProperty(int)),
        ('name', FileSystemProperty(unicode)),
        ('second_ap', FileSystemProperty(Item, relation="many-to-one",
            remote_ap="second_ap", remote_property="code"))),
        content_property="color")
    return UnicodeStream(fs_ap, 'color', 'utf-8')

@second_ap(teardown=teardown_fs)
def second_ap_fs():
    temp_dir = tempfile.mkdtemp()
    remote_prop = FileSystemProperty(iter, relation='one-to-many',
            remote_ap='first_ap',
            remote_property='second_ap')
    fs_ap = FileSystem(temp_dir, "(.*)", (
        ('code', FileSystemProperty(unicode)),
        ('first_aps', remote_prop)),
        content_property="name")
    return UnicodeStream(fs_ap, 'name', 'utf-8')
