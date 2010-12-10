from ..test_combinations import first_ap, second_ap
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.xml import XML, XMLProperty

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
    xml_ap = XML(fs_ap, [
        ('name' , XMLProperty(unicode, '//name')),
        ('color' , XMLProperty(unicode, '//color')),
        ('second_ap', XMLProperty(Item, '//second_ap', relation="many-to-one",
            remote_ap="second_ap", remote_property="code"))],
        'stream',
        'root')
    return xml_ap

@second_ap(teardown=teardown_fs)
def second_ap_fs():
    temp_dir = tempfile.mkdtemp()
    fs_ap = FileSystem(temp_dir, "(.*)",(
        ('code', FileSystemProperty(unicode)),),
        content_property="stream")
    xml_ap = XML(fs_ap, [
        ('name', XMLProperty(unicode, '//name')),
        ('first_aps', XMLProperty(iter, '//first_aps', relation="one-to-many",
            remote_ap="first_ap", remote_property="second_ap"))],
        'stream',
        'root')
    return xml_ap
