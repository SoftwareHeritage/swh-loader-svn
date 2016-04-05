# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import libloader


class TestLibLoader(unittest.TestCase):
    @istest
    def shallow_blob(self):
        # when
        actual_blob = libloader.shallow_blob({
            'length': 1451,
            'sha1_git':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'name': b'LDPCL',
            'type': b'blob',
            'sha256':
            b'\xe6it!\x99\xb37UT\x8f\x0e\x8f\xd7o\x92"\xce\xa3\x1d\xd2\xe5D>M\xaaj/\x03\x138\xad\x1b',  # noqa
            'perms': b'100644',
            'sha1':
            b'.\x18Y\xd6M\x8c\x9a\xa4\xe1\xf1\xc7\x95\x082\xcf\xc9\xd8\nV)',
            'path':
            b'/tmp/tmp.c86tq5o9.swh.loader/pkg-doc-linux/copyrights/non-free/LDPCL'  # noqa
        })

        # then
        self.assertEqual(actual_blob, {
            'sha1':
            b'.\x18Y\xd6M\x8c\x9a\xa4\xe1\xf1\xc7\x95\x082\xcf\xc9\xd8\nV)',
            'sha1_git':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'sha256':
            b'\xe6it!\x99\xb37UT\x8f\x0e\x8f\xd7o\x92"\xce\xa3\x1d\xd2\xe5D>M\xaaj/\x03\x138\xad\x1b',  # noqa
            'length': 1451,
        })

    @istest
    def shallow_tree(self):
        # when
        actual_shallow_tree = libloader.shallow_tree({
            'length': 1451,
            'sha1_git':
            b'tree-id',
            'type': b'tree',
            'sha256':
            b'\xe6it!\x99\xb37UT\x8f\x0e\x8f\xd7o\x92"\xce\xa3\x1d\xd2\xe5D>M\xaaj/\x03\x138\xad\x1b',  # noqa
            'perms': b'100644',
            'sha1':
            b'.\x18Y\xd6M\x8c\x9a\xa4\xe1\xf1\xc7\x95\x082\xcf\xc9\xd8\nV)',
        })

        # then
        self.assertEqual(actual_shallow_tree, b'tree-id')

    @istest
    def shallow_commit(self):
        # when
        actual_shallow_commit = libloader.shallow_commit({
            'sha1_git':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'type': b'commit',
            'id': b'let-me-see-some-id',
        })

        # then
        self.assertEqual(actual_shallow_commit, b'let-me-see-some-id')

    @istest
    def shallow_tag(self):
        # when
        actual_shallow_tag = libloader.shallow_tag({
            'sha1':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'type': b'tag',
            'id': b'this-is-not-the-id-you-are-looking-for',
        })

        # then
        self.assertEqual(actual_shallow_tag, b'this-is-not-the-id-you-are-looking-for')  # noqa
