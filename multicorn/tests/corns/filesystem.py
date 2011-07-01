# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import io
import os.path
import functools
import tempfile
import shutil
from contextlib import contextmanager

from attest import assert_hook
import attest.contexts

from multicorn.corns.filesystem import Filesystem
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.corns.extensers.typeextenser import TypeExtenser
from multicorn.requests import CONTEXT as c

from . import make_test_suite


def make_generic_corn():
    fs_corn = Filesystem(
        'raw_fs_corn',
        root_dir=tempfile.mkdtemp(),
        pattern='{id}_{name}.txt',
        content_property='lastname',
        encoding='utf8')
    @declare(TypeExtenser, wrapped_corn=fs_corn)
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn

def generic_corn_teardown(corn):
    shutil.rmtree(corn.root)

## Generic tests
# TODO: uncomment this when TypeExtenser is implemented.
#generic_suite = make_test_suite(make_generic_corn, 'filesystem',
#                                teardown=generic_corn_teardown)

# Filesystem-specific tests
specific_suite = attest.Tests(contexts=[attest.contexts.tempdir])



@contextmanager
def assert_raises(exception_class, message_part):
    """
    Check that an exception is raised and its message contains some string.
    """
    with attest.raises(exception_class) as exception:
        yield
    assert message_part.lower() in exception.args[0].lower()



@specific_suite.test
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


@specific_suite.test
def test_init(tempdir):
    binary = make_binary_corn(tempdir)
    text = make_text_corn(tempdir)
    assert set(binary.properties) == set(['category', 'num', 'name', 'data'])
    assert set(text.properties) == set(['category', 'num', 'name', 'content'])


@specific_suite.test
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


@specific_suite.test
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

@specific_suite.test
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


@specific_suite.test
def test_request(tempdir):
    text, item = make_populated_text_corn(tempdir)

    re_item = text.all.one().execute()
    # Same values, but not the same object.
    values = dict(
        category='lipsum', num='4', name='foo', content=u'Héllö World!')
    assert dict(item) == values
    assert dict(re_item) == values
    assert re_item is not item


@specific_suite.test
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


@specific_suite.test
def test_optimizations(tempdir):
    corn = Filesystem(
        'test_corn',
        root_dir=tempdir,
        pattern='{cat}/{org}_{name}/{id}')

    listed = []
    real_listdir = corn._listdir

    def listdir_mock(parts):
        listed.append('/'.join(parts))
        return real_listdir(parts)

    corn._listdir = listdir_mock

    contents = {}
    def create(**values):
        item = corn.create(dict(values, content=bytes()))
        content = item.filename.encode('ascii')
        assert values['id'] not in contents # Make sure ids are unique
        item['content'] = content
        item.save()
        contents[values['id']] = content

    def assert_listed(request, expected_ids, expected_listed):
        del listed[:]
        expected_contents = set(contents[num] for num in expected_ids)
        results = request.map(c.content).execute()
        assert set(results) == expected_contents
        # Workaround a bug in Attest’s assert hook with closures
        listed_ = listed
        assert set(listed_) == set(expected_listed)

    create(cat='lipsum', org='a', name='foo', id='1')

    # No fixed values: all directories on the path are listed.
    assert_listed(corn.all,
        ['1'], ['', 'lipsum', 'lipsum/a_foo'])

    # The category was fixed, no need to listdir() the root.
    assert_listed(corn.all.filter(cat='lipsum'),
        ['1'], ['lipsum', 'lipsum/a_foo'])

    # The num and name were fixed, no need to listdir() the lipsum dir.
    assert_listed(corn.all.filter(org='a', name='foo'),
        ['1'], ['', 'lipsum/a_foo'])

    # All filename properties were fixed, no need to listdir() anything
    assert_listed(corn.all.filter(cat='lipsum', org='a', name='foo', id='1'),
        ['1'], [])

    create(cat='lorem ipsum', org='b', name='foo', id='2')
    create(cat='lorem ipsum', org='c', name='bar', id='3')
    create(cat='lipsum dolor', org='d', name='bar', id='4')

    assert_listed(corn.all, ['1', '2', '3', '4'],
        ['', 'lipsum', 'lorem ipsum', 'lipsum dolor', 'lipsum/a_foo',
         'lorem ipsum/b_foo', 'lorem ipsum/c_bar', 'lipsum dolor/d_bar'])

    assert_listed(corn.all.filter(c.cat == 'lipsum'),
        ['1'], ['lipsum', 'lipsum/a_foo'])

    assert_listed(corn.all.filter(c.cat[:6] == 'lipsum'),
        ['1', '4'], ['', 'lipsum', 'lipsum dolor', 'lipsum/a_foo',
                     'lipsum dolor/d_bar'])

    assert_listed(corn.all.filter(c.cat[:2] != 'li', cat='lipsum'),
        [], [])

    # Does not list the root and directry tries to list 'nonexistent'
    assert_listed(corn.all.filter(cat='nonexistent'),
        [], ['nonexistent'])

    # The (un-splitable) "or" predicate rules out 'lorem ipsum/b_foo'.
    assert_listed(
        corn.all.filter(((c.cat[:2] == 'li') | (c.org == 'c')) &
                        (c.id >= '3')),
        ['3', '4'],
        ['', 'lipsum', 'lipsum dolor', 'lorem ipsum', 'lipsum dolor/d_bar',
         'lorem ipsum/c_bar', 'lipsum/a_foo'])
