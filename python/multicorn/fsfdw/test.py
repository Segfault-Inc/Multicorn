# coding: utf8

"""

Tests for StructuredFS.

"""


import os
import functools
from contextlib import contextmanager

import attest
from attest import Tests, assert_hook

from .structuredfs import StructuredDirectory, Item


SUITE = Tests(contexts=[attest.contexts.tempdir])


@contextmanager
def assert_raises(exception_class, message_part):
    """
    Check that an exception is raised and its message contains some string.
    """
    with attest.raises(exception_class) as exception:
        yield
    assert message_part.lower() in exception.args[0].lower()


@SUITE.test
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
    assert [regex.pattern for regex in bin._path_parts_re] \
        == ['^(?P<category>.*)$', r'^\{num\}\_(?P<name>.*)\.bin$']


@SUITE.test
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


@SUITE.test
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
    text.create(category='lipsum', num='5', name='bar').write('BAR')

    item_foo, item_bar, = sorted(text.get_items(),
                                 key=lambda item: item['num'])
    assert len(item_foo) == 3
    assert dict(item_foo) == dict(category='lipsum', num='4', name='foo')
    assert item_foo.read() == ''

    assert len(item_bar) == 3
    assert dict(item_bar) == dict(category='lipsum', num='5', name='bar')
    assert item_bar.read() == 'BAR'

    content = u'Hello, Wörld!'
    with attest.raises(UnicodeError):
        item_foo.write(content)
    item_foo.write(content.encode('utf8'))
    assert item_foo.read().decode('utf8') == content
    item_foo.remove()
    with attest.raises(IOError):
        item_foo.read()
    with attest.raises(OSError):
        item_foo.remove()

    assert [i.filename for i in text.get_items()] == ['lipsum/5_bar.txt']
    item_bar.remove()
    assert [i.filename for i in text.get_items()] == []
    # The 'lipsum' directory was also removed
    assert os.listdir(tempdir) == []


@SUITE.test
def test_get_items(tempdir):
    """
    Test the results of :meth:`StructuredDirectory.get_items`
    """
    text = StructuredDirectory(tempdir, '{category}/{num}_{name}.txt')

    text.create(category='lipsum', num='4', name='foo').write('FOO')
    text.create(category='lipsum', num='5', name='bar').write('BAR')

    def filenames(**properties):
        return [i.filename for i in text.get_items(**properties)]

    assert filenames(num='9') == []
    assert filenames(num='5', name='UUU') == []
    assert filenames(num='5') == ['lipsum/5_bar.txt']
    assert filenames(num='5', name='bar') == ['lipsum/5_bar.txt']
    assert sorted(filenames()) == ['lipsum/4_foo.txt', 'lipsum/5_bar.txt']

    with assert_raises(ValueError, 'Unknown properties'):
        filenames(fiz='5')

@SUITE.test
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



@SUITE.test
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
        item.write(content)
        contents[values['id']] = content

    def assert_listed(properties, expected_ids, expected_listed):
        del listed[:]
        expected_contents = set(contents[num] for num in expected_ids)
        results = [item.read() for item in text.get_items(**properties)]
        assert set(results) == expected_contents
        # Workaround a bug in Attest’s assert hook with closures
        listed_ = listed
        assert set(listed_) == set(expected_listed)

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
