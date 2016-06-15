# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import utils


class TestUtils(unittest.TestCase):
    @istest
    def strdate_to_timestamp(self):
        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:04:39.800722Z')

        self.assertEquals(actual_ts,
                          {'timestamp': 1306821879.800722, 'offset': 0})

    @istest
    def strdate_to_timestamp_empty_does_not_break(self):
        # It should return 0, epoch
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          utils.strdate_to_timestamp(''))
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          utils.strdate_to_timestamp(None))


class TestHashesConvert(unittest.TestCase):
    def setUp(self):
        self.hashes = {
            b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox': {
                'checksums': {
                    'name': b'pkg-fox',
                    'sha1_git': b'\xad\xdf2x\x1fBX\xdb\xe8Adt\xc9\xf5~\xcb6\x98^\xbf',  # noqa
                    'path': b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox'
                },
                'children': {
                    b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/fox-1.2',
                    b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/fox-1.4'
                }
            },
            b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/fox-1.4': {
                'checksums': 'something',
                'children': set()
            },
            b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/fox-1.2': {
                'checksums': 'something'
            },
            b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/fox-1.3': {
                'checksums': 'or something',
                'children': {
                    b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/some/path'
                }
            }
        }

        self.expected_output = {
            b'': {
                'checksums': {
                    'name': b'pkg-fox',
                    'sha1_git': b'\xad\xdf2x\x1fBX\xdb\xe8Adt\xc9\xf5~\xcb6\x98^\xbf',  # noqa
                    'path': b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox'
                },
                'children': {
                    b'fox-1.2', b'fox-1.4'
                }
            },
            b'fox-1.4': {
                'checksums': 'something',
                'children': set()
            },
            b'fox-1.2': {
                'checksums': 'something',
            },
            b'fox-1.3': {
                'checksums': 'or something',
                'children': {
                    b'some/path'
                }
            }
        }

    @istest
    def convert_hashes_with_relative_path(self):

        actual_output = utils.convert_hashes_with_relative_path(
            self.hashes,
            b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox')

        self.assertEquals(actual_output, self.expected_output)

    @istest
    def convert_hashes_with_relative_path_with_slash(self):
        actual_output = utils.convert_hashes_with_relative_path(
            self.hashes,
            b'/tmp/tmp.c39vkrp1.swh.loader/pkg-fox/')

        self.assertEquals(actual_output, self.expected_output)
