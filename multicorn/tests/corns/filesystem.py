# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

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

    assert make_corn('{category}/{num}_{name}.bin').identity_properties \
        == ('category', 'num', 'name')
    bin = make_corn('{category}/{{num}}_{name}.bin')
    assert bin.identity_properties == ('category', 'name')
    assert bin._path_parts_re == ('^(?P<category>.*)$',
                                  r'^\{num\}\_(?P<name>.*)\.bin$')

def make_raw_fs(root):
    mc = Multicorn()

    binary = Filesystem(
        'binary',
        root_dir=root,
        pattern='{category}/{num}_{name}.bin',
        content_property_name='data')
    text = Filesystem(
        'text',
        root_dir=root,
        pattern='{category}/{num}_{name}.txt',
        encoding='utf8')

    mc.register(binary)
    mc.register(text)

    return binary, text


@suite.test
def test_init(tempdir):
    binary, text = make_raw_fs(tempdir)
    assert set(binary.properties) == set(['category', 'num', 'name', 'data'])
    assert set(text.properties) == set(['category', 'num', 'name', 'content'])

    item = dict(category='lipsum', num=4, name='foo')
    with assert_raises(TypeError, 'must be of type unicode'):
        binary._filename_from_item(item)

    item['num'] = '4'
    assert binary._filename_from_item(item) == 'lipsum/4_foo.bin'
    assert text._filename_from_item(item) == 'lipsum/4_foo.txt'

    assert binary._values_from_filename('lipsum/4_foo.bin') == item
    assert text._values_from_filename('lipsum/4_foo.txt') == item

    assert binary._values_from_filename('lipsum/4_foo.txt') is None
    assert text._values_from_filename('lipsum/4_foo.bin') is None
    assert text._values_from_filename('lipsum/4_foo') is None
    assert text._values_from_filename('lipsum/4-foo.txt') is None
    assert text._values_from_filename('lipsum/4_foo.txt/baz') is None
    assert text._values_from_filename('lipsum/4') is None
    assert text._values_from_filename('lipsum/') is None
    assert text._values_from_filename('lipsum') is None
