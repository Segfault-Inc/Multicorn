# coding: utf8

"""

Tests for StructuredFS.

"""


import os
import sys
import functools
import tempfile
import shutil
from contextlib import contextmanager
from multicorn.compat import unicode_, bytes_
import pytest

from .structuredfs import StructuredDirectory, Item
from .docutils_meta import mtime_lru_cache, extract_meta


def with_tempdir(function):
    def wrapper():
        directory = tempfile.mkdtemp()
        try:
            return function(directory)
        finally:
            shutil.rmtree(directory)
    wrapper.__doc__ = function.__doc__
    wrapper.__name__ = function.__name__
    return wrapper


@contextmanager
def assert_raises(exception_class, message_part):
    """
    Check that an exception is raised and its message contains some string.
    """
    try:
        yield
    except exception_class as exception:
        assert message_part.lower() in exception.args[0].lower()
    else:
        assert 0, 'Did not raise %s' % exception_class


@with_tempdir
def test_parser(tempdir):
    """
    Test the pattern parser.
    """
    make = functools.partial(StructuredDirectory, tempdir)

    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make('')
    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make('/a')
    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make('a/')
    with assert_raises(ValueError, 'slash-separated part is empty'):
        assert make('a//b')
    with assert_raises(ValueError, 'more than once'):
        assert make('{foo}/{foo}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make('{}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make('{0foo}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make('{foo/bar}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make('{foo!r}')
    with assert_raises(ValueError, 'Invalid property name'):
        assert make('{foo:s}')
    with assert_raises(ValueError, "unmatched '{'"):
        assert make('foo{bar')
    with assert_raises(ValueError, "single '}'"):
        assert make('foo}bar')

    bin = make('{category}/{num}_{name}.bin')
    assert bin.properties == set(['category', 'num', 'name'])
    assert bin._path_parts_properties == (('category',), ('num', 'name'))

    bin = make('{category}/{{num}}_{name}.bin')
    assert bin.properties == set(['category', 'name'])
    assert bin._path_parts_properties == (('category',), ('name',))


@with_tempdir
def test_filenames(tempdir):
    binary = StructuredDirectory(tempdir, '{category}/{num}_{name}.bin')
    text = StructuredDirectory(tempdir, '{category}/{num}_{name}.txt')

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

    assert [i.filename for i in text.get_items()] == ['lipsum/4_foo.txt']
    assert [i.filename for i in binary.get_items()] == ['lipsum/4_foo.bin']


@with_tempdir
def test_items(tempdir):
    """
    Test the :class:`Item` class.
    """
    binary = StructuredDirectory(tempdir, '{category}/{num}_{name}.bin')
    text = StructuredDirectory(tempdir, '{category}/{num}_{name}.txt')

    with assert_raises(ValueError, 'Missing properties'):
        text.create(category='lipsum')

    with assert_raises(ValueError, 'Unknown properties'):
        text.create(category='lipsum', num='4', name='foo', bang='bar')

    with assert_raises(TypeError, 'must be of type unicode'):
        text.create(category='lipsum', num=4, name='foo')

    with assert_raises(ValueError, 'can not contain a slash'):
        text.create(category='lipsum', num='4', name='foo/bar')

    values = dict(category='lipsum', num='4', name='foo')
    assert Item(binary, values).filename == 'lipsum/4_foo.bin'
    assert Item(text, values).filename == 'lipsum/4_foo.txt'

    # No file created yet
    assert os.listdir(tempdir) == []

    # Create a file directly
    os.mkdir(os.path.join(text.root_dir, 'lipsum'))
    open(os.path.join(text.root_dir, 'lipsum', '4_foo.txt'), 'wb').close()

    # Create a file from an Item
    i1 = text.create(category='lipsum', num='5', name='bar')
    i1.content = 'BAR'
    i1.write()

    item_foo, item_bar, = sorted(text.get_items(),
                                 key=lambda item: item['num'])
    assert len(item_foo) == 3
    assert dict(item_foo) == dict(category='lipsum', num='4', name='foo')
    assert item_foo.read() == bytes_('')

    assert len(item_bar) == 3
    assert dict(item_bar) == dict(category='lipsum', num='5', name='bar')
    assert item_bar.read() == bytes_('BAR')

    content = b'Hello,\xc2\xa0W\xc3\xb6rld!'.decode('utf-8')
    item_foo.content = content
    item_foo.write()
    assert item_foo.read().decode('utf8') == content
    item_foo.content = content.encode('utf8')
    item_foo.write()
    assert item_foo.read().decode('utf8') == content
    item_foo.remove()
    with pytest.raises(OSError):
        item_foo.remove()

    assert [i.filename for i in text.get_items()] == ['lipsum/5_bar.txt']
    item_bar.remove()
    assert [i.filename for i in text.get_items()] == []


@with_tempdir
def test_get_items(tempdir):
    """
    Test the results of :meth:`StructuredDirectory.get_items`
    """
    text = StructuredDirectory(tempdir, '{category}/{num}_{name}.txt')

    i1 = text.create(category='lipsum', num='4', name='foo')
    i1.content = 'FOO'
    i1.write()
    i2 = text.create(category='lipsum', num='5', name='bar')
    i2.content = 'BAR'
    i2.write()

    def filenames(**properties):
        return [i.filename for i in text.get_items(**properties)]

    assert filenames(num='9') == []
    assert filenames(num='5', name='UUU') == []
    assert filenames(num='5') == ['lipsum/5_bar.txt']
    assert filenames(num='5', name='bar') == ['lipsum/5_bar.txt']
    assert sorted(filenames()) == ['lipsum/4_foo.txt', 'lipsum/5_bar.txt']

    with assert_raises(ValueError, 'Unknown properties'):
        filenames(fiz='5')


@with_tempdir
def test_from_filename(tempdir):
    """
    Test the results of :meth:`StructuredDirectory.from_filename`
    """
    text = StructuredDirectory(tempdir, '{category}/{num}_{name}.txt')

    assert text.from_filename('lipsum/4_foo.txt/bar') is None
    assert text.from_filename('lipsum') is None
    assert text.from_filename('lipsum/4') is None
    assert text.from_filename('lipsum/4_foo.bin') is None
    matching = text.from_filename('lipsum/4_foo.txt')
    assert dict(matching) == dict(category='lipsum', num='4', name='foo')
    assert matching.filename == 'lipsum/4_foo.txt'


@with_tempdir
def test_optimizations(tempdir):
    """
    Test that :meth:`StructuredDirectory.get_items` doesn’t do more calls
    to :func:`os.listdir` than needed.
    """
    text = StructuredDirectory(tempdir, '{cat}/{org}_{name}/{id}')

    listed = []
    real_listdir = text._listdir

    def listdir_mock(parts):
        listed.append('/'.join(parts))
        return real_listdir(parts)

    text._listdir = listdir_mock

    contents = {}

    def create(**values):
        item = Item(text, values)
        assert values['id'] not in contents  # Make sure ids are unique
        content = item.filename.encode('ascii')
        item.content = content
        item.write()
        contents[values['id']] = content

    def assert_listed(properties, expected_ids, expected_listed):
        del listed[:]
        expected_contents = set(contents[num] for num in expected_ids)
        results = [item.read() for item in text.get_items(**properties)]
        assert set(results) == expected_contents
        assert set(listed) == set(expected_listed)

    create(cat='lipsum', org='a', name='foo', id='1')

    # No fixed values: all directories on the path are listed.
    assert_listed(dict(),
        ['1'],
        ['', 'lipsum', 'lipsum/a_foo'])

    # The category was fixed, no need to listdir() the root.
    assert_listed(dict(cat='lipsum'),
        ['1'],
        ['lipsum', 'lipsum/a_foo'])

    # The num and name were fixed, no need to listdir() the lipsum dir.
    assert_listed(dict(org='a', name='foo'),
        ['1'],
        ['', 'lipsum/a_foo'])

    # All filename properties were fixed, no need to listdir() anything
    assert_listed(dict(cat='lipsum', org='a', name='foo', id='1'),
        ['1'],
        [])

    create(cat='lipsum', org='b', name='foo', id='2')
    create(cat='dolor', org='c', name='bar', id='3')

    assert_listed(dict(),
        ['1', '2', '3'],
        ['', 'lipsum', 'dolor', 'lipsum/a_foo', 'lipsum/b_foo', 'dolor/c_bar'])

    # No need to listdir() the root
    assert_listed(dict(cat='lipsum'),
        ['1', '2'],
        ['lipsum', 'lipsum/a_foo', 'lipsum/b_foo'])

    # No need to listdir() the root
    assert_listed(dict(cat='dolor'),
        ['3'],
        ['dolor', 'dolor/c_bar'])

    # org='b' is not a whole part so we still need to listdir() lipsum,
    # but can filter out some deeper directories
    assert_listed(dict(org='b'),
        ['2'],
        ['', 'lipsum', 'dolor', 'lipsum/b_foo'])

    # Does not list the root and directry tries to list 'nonexistent'
    assert_listed(dict(cat='nonexistent'),
        [],
        ['nonexistent'])


@with_tempdir
def test_docutils_meta(tempdir):
    def counting(filename):
        counting.n_calls += 1
        return extract_meta(filename)
    counting.n_calls = 0
    wrapper = mtime_lru_cache(counting, max_size=2)
    def extract(filename):
        return wrapper(os.path.join(tempdir, filename))
    rest_1 = '''
The main title
==============

Second title
------------

:Author: Me

Content
'''
    meta_1 = {'title': 'The main title', 'subtitle': 'Second title',
              'author': 'Me'}
    rest_2 = '''
First title
===========

:Author: Myself
:Summary:
    Lorem ipsum
    dolor sit amet

Not a subtitle
--------------

Content
'''
    meta_2 = {'title': 'First title', 'author': 'Myself',
              'summary': 'Lorem ipsum\ndolor sit amet'}
    def write(filename, content):
        with open(os.path.join(tempdir, filename), 'w') as file_obj:
            file_obj.write(content)
    write('first.rst', rest_1)
    write('second.rst', rest_2)
    assert counting.n_calls == 0
    assert extract('first.rst') == meta_1
    assert counting.n_calls == 1
    assert extract('first.rst') == meta_1  # cached
    assert counting.n_calls == 1
    assert extract('second.rst') == meta_2
    assert counting.n_calls == 2
    write('third.rst', rest_1)
    assert extract('third.rst') == meta_1  # Exceeds the cache size
    assert counting.n_calls == 3
    write('third.rst', rest_2)
    assert extract('third.rst') == meta_2
    assert counting.n_calls == 4
    assert extract('first.rst') == meta_1  # Not cached anymore
    assert counting.n_calls == 5


if __name__ == '__main__':
    pytest.main([__file__] + sys.argv)
