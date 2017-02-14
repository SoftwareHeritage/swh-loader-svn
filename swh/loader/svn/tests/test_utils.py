# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import unittest

from nose.tools import istest
from test_base import BaseTestTreeLoader

from swh.loader.svn import utils

from swh.model import git


class TestUtils(unittest.TestCase):
    @istest
    def strdate_to_timestamp(self):
        """Formatted string date should be converted in timestamp."""
        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:04:39.800722Z')
        self.assertEquals(actual_ts, {'seconds': 1306821879,
                                      'microseconds': 800722})

        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:03:39.123450Z')
        self.assertEquals(actual_ts, {'seconds': 1306821819,
                                      'microseconds': 123450})

    @istest
    def strdate_to_timestamp_empty_does_not_break(self):
        """Empty or None date should be timestamp 0."""
        self.assertEquals({'seconds': 0, 'microseconds': 0},
                          utils.strdate_to_timestamp(''))
        self.assertEquals({'seconds': 0, 'microseconds': 0},
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


class HashtreeITTest(BaseTestTreeLoader):
    @istest
    def hashtree_not_existing_path(self):
        # path does not exist
        with self.assertRaises(ValueError):
            utils.hashtree('/not/exists', ignore_empty_folder=False)

    @istest
    def hashtree_not_a_dir(self):
        fpath = '/tmp/foobar'
        with open(fpath, 'wb') as f:
            f.write(b'foo')

        # path is not a folder
        with self.assertRaises(ValueError):
            utils.hashtree(fpath, ignore_empty_folder=True)

        os.unlink(fpath)

    @istest
    def hashtree_with_empty_folder(self):
        # not ignoring empty folder
        # no pattern to ignore
        # this is the base case
        root_hash = self.tmp_root_path.encode('utf-8')
        actual_hash = utils.hashtree(root_hash,
                                     ignore_empty_folder=False)

        expected_hashes = git.compute_hashes_from_directory(
            self.tmp_root_path.encode('utf-8'))

        expected_hash = expected_hashes[root_hash]['checksums']['sha1_git']
        self.assertEquals(actual_hash['sha1_git'], expected_hash)

    @istest
    def hashtree_ignore_pattern_with_empty_folder(self):
        # not ignoring empty folder
        # 'empty-folder' pattern to ignore
        root_hash = self.tmp_root_path.encode('utf-8')
        actual_hash = utils.hashtree(root_hash,
                                     ignore_empty_folder=False,
                                     ignore=['empty-folder'])

        expected_hashes = git.compute_hashes_from_directory(
            self.tmp_root_path.encode('utf-8'),
            dir_ok_fn=lambda dp: b'empty-folder' not in dp)

        expected_hash = expected_hashes[root_hash]['checksums']['sha1_git']
        self.assertEquals(actual_hash['sha1_git'], expected_hash)

    @istest
    def hashtree_ignore_pattern_no_empty_folder(self):
        # ignoring empty folder
        # '/barfoo/' pattern to ignore
        root_hash = self.tmp_root_path.encode('utf-8')
        actual_hash = utils.hashtree(root_hash,
                                     ignore_empty_folder=True,
                                     ignore=['/barfoo/'])

        def ignore_fn(dp):
            return b'/barfoo/' not in dp

        expected_hashes = git.compute_hashes_from_directory(
            self.tmp_root_path.encode('utf-8'),
            dir_ok_fn=ignore_fn,
            remove_empty_folder=True)

        expected_hash = expected_hashes[root_hash]['checksums']['sha1_git']
        self.assertEquals(actual_hash['sha1_git'], expected_hash)

    @istest
    def hashtree_no_ignore_pattern_no_empty_folder(self):
        # ignoring empty folder
        root_hash = self.tmp_root_path.encode('utf-8')
        actual_hash = utils.hashtree(root_hash,
                                     ignore_empty_folder=True)

        expected_hashes = git.compute_hashes_from_directory(
            self.tmp_root_path.encode('utf-8'),
            remove_empty_folder=True)

        expected_hash = expected_hashes[root_hash]['checksums']['sha1_git']
        self.assertEquals(actual_hash['sha1_git'], expected_hash)
