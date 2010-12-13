from ..test_combinations import first_ap, second_ap
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.xml.rest import Rest, RestProperty

import tempfile
import shutil


def teardown_fs(access_point):
    shutil.rmtree(access_point.wrapped_ap.root_dir)

@first_ap(teardown=teardown_fs)
def first_ap_fs():
    temp_dir = tempfile.mkdtemp()
    fs_ap = FileSystem(temp_dir, "(.*)",(
        ('id', FileSystemProperty(int)),),
        content_property="stream")
    rest_ap = Rest(fs_ap, [
        ('name' , RestProperty(unicode, '//title')),
        ('color' , RestProperty(unicode, '//subtitle')),
        ('second_ap', RestProperty(Item, '//paragraph', relation="many-to-one",
            remote_ap="second_ap", remote_property="code"))],
        'stream',
        )
    return rest_ap

@second_ap(teardown=teardown_fs)
def second_ap_fs():
    temp_dir = tempfile.mkdtemp()
    fs_ap = FileSystem(temp_dir, "(.*)",(
        ('code', FileSystemProperty(unicode)),),
        content_property="stream")
    rest_ap = Rest(fs_ap, [
        ('name' , RestProperty(unicode, '//title')),
        ('first_aps', RestProperty(iter, '//paragraph/substitution_reference', relation="one-to-many",
            remote_ap="first_ap", remote_property="second_ap"))],
        'stream')
    return rest_ap
