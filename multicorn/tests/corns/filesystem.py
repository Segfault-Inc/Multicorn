# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import io
import os.path
import functools
from contextlib import contextmanager

from attest import assert_hook
import attest.contexts

from multicorn.corns.filesystem import Filesystem
from multicorn import Multicorn
from multicorn.requests import CONTEXT as c

from . import make_test_suite


@contextmanager
def assert_raises(exception_class, message_part):
    """
    Check that an exception is raised and its message contains some string.
    """
    with attest.raises(exception_class) as exception:
        yield
    assert message_part.lower() in exception.args[0].lower()


# Filesystem-specific tests
suite = attest.Tests(contexts=[attest.contexts.tempdir])


@suite.test
def test_parser(tempdir):
    make_corn = functools.partial(Filesystem, 'test_corn', tempdir)

    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make_corn('')
    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make_corn('/a')
    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make_corn('a/')
    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make_corn('a//b')
    with assert_raises(ValueError, 'more than once'):
        assert make_corn('{foo}/{foo}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make_corn('{}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make_corn('{0foo}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make_corn('{foo/bar}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make_corn('{foo!r}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make_corn('{foo:s}')
    with assert_raises(ValueError, "unmatched '{'"):
        assert make_corn('foo{bar')
    with assert_raises(ValueError, "single '}'"):
        assert make_corn('foo}bar')

    bin = make_corn('{category}/{num}_{name}.bin')
    assert bin.identity_properties == ('category', 'num', 'name')
    assert bin._path_parts_properties == (('category',), ('num', 'name'))

    bin = make_corn('{category}/{{num}}_{name}.bin')
    assert bin.identity_properties == ('category', 'name')
    assert bin._path_parts_properties == (('category',), ('name',))
    assert [regex.pattern for regex in bin._path_parts_re] \
        == ['^(?P<category>.*)$', r'^\{num\}\_(?P<name>.*)\.bin$']

def make_binary_corn(root):
    return Filesystem(
        'binary',
        root_dir=root,
        pattern='{category}/{num}_{name}.bin',
        content_property='data')


def make_text_corn(root):
    return Filesystem(
        'text',
        root_dir=root,
        pattern='{category}/{num}_{name}.txt',
        encoding='utf8')


def make_populated_text_corn(root):
    text = make_text_corn(root)
    item = text.create(dict(
        category='lipsum', num='4', name='foo', content=u'Héllö World!'))
    item.save()
    return text, item


@suite.test
def test_init(tempdir):
    binary = make_binary_corn(tempdir)
    text = make_text_corn(tempdir)
    assert set(binary.properties) == set(['category', 'num', 'name', 'data'])
    assert set(text.properties) == set(['category', 'num', 'name', 'content'])


@suite.test
def test_filenames(tempdir):
    binary = make_binary_corn(tempdir)
    text = make_text_corn(tempdir)

    values = dict(category='lipsum', num=4, name='foo')
    with assert_raises(TypeError, 'must be of type unicode'):
        binary.create(values).filename

    values['num'] = '4'
    assert binary.create(values).filename == 'lipsum/4_foo.bin'
    assert text.create(values).filename == 'lipsum/4_foo.txt'

    # No file created yet
    assert os.listdir(tempdir) == []

    # Create some files
    for path_parts in [
            # Matching the pattern
            ['lipsum', '4_foo.bin'],
            ['lipsum', '4_foo.txt'],

            # Not matching the pattern
            ['lipsum', '4_foo'],
            ['lipsum', '4-foo.txt'],
            ['lipsum', '4_bar.txt', 'baz'],
            ['lipsum', '4'],
            ['dolor']]:
        filename = os.path.join(tempdir, *path_parts)
        dirname = os.path.dirname(filename)
        # Create parent directories as needed
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        # Create an empty file
        open(filename, 'wb').close()

    assert [i.filename for i in text.all.execute()] == ['lipsum/4_foo.txt']
    assert [i.filename for i in binary.all.execute()] == ['lipsum/4_foo.bin']


@suite.test
def test_save(tempdir):
    binary = make_binary_corn(tempdir)
    text = make_text_corn(tempdir)

    data = b'\x01\x02\x03'
    item1 = binary.create(dict(
        category='lipsum', num='4', name='foo', data=data))
    item1.save()
    with open(item1.full_filename, 'rb') as fd:
        assert fd.read() == data

    content = u'Héllö World!'
    item2 = text.create(dict(
        category='lipsum', num='4', name='foo', content=content))
    item2.save()
    with open(item2.full_filename, 'rb') as fd:
        assert fd.read() == content.encode('utf8')

@suite.test
def test_delete(tempdir):
    text, item = make_populated_text_corn(tempdir)

    filename = os.path.join(text.root_dir, 'lipsum', '4_foo.txt')
    dirname = os.path.join(text.root_dir, 'lipsum')
    assert os.path.isfile(filename)
    assert os.path.isdir(dirname)

    item.delete()

    assert not os.path.exists(filename)
    # Directories are also removed.
    assert not os.path.exists(dirname)
    # But the corn’s root is kept.
    assert os.path.isdir(text.root_dir)


@suite.test
def test_request(tempdir):
    text, item = make_populated_text_corn(tempdir)

    re_item = text.all.one().execute()
    # Same values, but not the same object.
    values = dict(
        category='lipsum', num='4', name='foo', content=u'Héllö World!')
    assert dict(item) == values
    assert dict(re_item) == values
    assert re_item is not item


@suite.test
def test_laziness(tempdir):
    text, item = make_populated_text_corn(tempdir)

    def io_open_mock(filename, *args, **kwargs):
        files_opened.append(filename)
        return real_io_open(filename, *args, **kwargs)

    files_opened = []
    real_io_open = io.open
    io.open = io_open_mock

    try:
        re_item = text.all.one().execute()
        assert re_item['category'] == 'lipsum'

        # The file was not read yet
        assert files_opened == []
        # Reading lazily
        assert re_item['content'] == u'Héllö World!'
        assert files_opened == [item.full_filename]
    finally:
        io.open = real_io_open


@suite.test
def test_optimizations(tempdir):
    text, item = make_populated_text_corn(tempdir)

    def get(request):
        re_item = request.one().execute()
        # Workaround a bug in Attest’s assert hook with closures
        item_ = item
        # We already tested in test_request() that dict(some_item) has
        # all the values.
        assert dict(re_item) == dict(item_)

    def os_listdir_mock(dirname):
        listed.append(dirname)
        return real_os_listdir(dirname)

    real_os_listdir = os.listdir
    os.listdir = os_listdir_mock

    try:
        listed = []
        get(text.all)
        # No fixed values: all directories on the path are listed.
        assert listed == [tempdir, os.path.join(tempdir, 'lipsum')]

        listed = []
        get(text.all.filter(category='lipsum'))
        # The category was fixed, no need to listdir() the root.
        assert listed == [os.path.join(tempdir, 'lipsum')]

        listed = []
        get(text.all.filter(num='4', name='foo'))
        # The num and name were fixed, no need to listdir() the lipsum dir.
        assert listed == [tempdir]

        listed = []
        get(text.all.filter(category='lipsum', num='4', name='foo'))
        # All filename properties were fixed, no need to listdir() anything
        assert listed == []
        
        # TODO: More tests for non-fixed value predicates
    finally:
        os.listdir = real_os_listdir
