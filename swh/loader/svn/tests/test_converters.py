# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import converters


class TestConverters(unittest.TestCase):
    @istest
    def svn_author_to_person(self):
        actual_person = converters.svn_author_to_person(
            'tony <ynot@dagobah>',
            repo_uuid=None)
        self.assertEquals(actual_person, {
            'fullname': b'tony <ynot@dagobah>',
            'name': b'tony',
            'email': b'ynot@dagobah',
        })

    @istest
    def svn_author_to_person_no_email(self):
        # should not happen - input is bytes but nothing prevents it
        actual_person = converters.svn_author_to_person('tony',
                                                        repo_uuid=b'some-uuid')
        self.assertEquals(actual_person, {
            'fullname': b'tony <tony@some-uuid>',
            'name': b'tony',
            'email': b'tony@some-uuid',
        })

    @istest
    def svn_author_to_person_empty_person(self):
        actual_person = converters.svn_author_to_person('',
                                                        repo_uuid=b'some-uuid')
        self.assertEqual(actual_person, {
            'fullname': b' <@some-uuid>',
            'name': b'',
            'email': b'@some-uuid'
        })

    @istest
    def build_swh_revision_default(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid=b'uuid',
            dir_id='dir-id',
            commit={
                'author_name': {
                    'name': b'theo',
                    'email': b'theo@uuid',
                    'fullname': b'theo <theo@uuid>'
                },
                'message': b'commit message',
                'author_date': {
                    'timestamp': 1088108379,
                    'offset': 0
                }
            },
            rev=10,
            parents=['123'])

        date = {'timestamp': 1088108379, 'offset': 0}

        self.assertEquals(actual_swh_revision, {
            'date': date,
            'committer_date': date,
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'commit message',
            'author': {
                'name': b'theo',
                'email': b'theo@uuid',
                'fullname': b'theo <theo@uuid>'
            },
            'committer': {
                'name': b'theo',
                'email': b'theo@uuid',
                'fullname': b'theo <theo@uuid>'
            },
            'synthetic': True,
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', b'uuid'],
                    ['svn_revision', b'10'],
                ]
            },
            'parents': ['123'],
        })

    @istest
    def build_swh_revision_no_extra_headers(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid=b'uuid',
            dir_id='dir-id',
            commit={
                'author_name': {
                    'name': b'theo',
                    'email': b'theo@uuid',
                    'fullname': b'theo <theo@uuid>'
                },
                'message': b'commit message',
                'author_date': {
                    'timestamp': 1088108379,
                    'offset': 0
                }
            },
            rev=10,
            parents=['123'],
            with_revision_headers=False)

        date = {'timestamp': 1088108379, 'offset': 0}

        self.assertEquals(actual_swh_revision, {
            'date': date,
            'committer_date': date,
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'commit message',
            'author': {
                'name': b'theo',
                'email': b'theo@uuid',
                'fullname': b'theo <theo@uuid>'
            },
            'committer': {
                'name': b'theo',
                'email': b'theo@uuid',
                'fullname': b'theo <theo@uuid>'
            },
            'synthetic': True,
            'metadata': None,
            'parents': ['123'],
        })

    @istest
    def build_swh_revision_empty_data(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid=b'uuid',
            dir_id='dir-id',
            commit={
                'author_name': {
                    'fullname': b'',
                    'name': None,
                    'email': None
                },
                'message': b'',
                'author_date': {
                    'timestamp': 1088108379,
                    'offset': 0
                }
            },
            rev=8,
            parents=[])

        date = {
            'timestamp': 1088108379,
            'offset': 0
        }

        author = {
            'fullname': b'',
            'name': None,
            'email': None
        }
        self.assertEquals(actual_swh_revision, {
            'date': date,
            'committer_date': date,
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'',
            'author': author,
            'committer': author,
            'synthetic': True,
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', b'uuid'],
                    ['svn_revision', b'8'],
                ]
            },
            'parents': [],
        })

    @istest
    def build_swh_occurrence(self):
        actual_occ = converters.build_swh_occurrence('revision-id',
                                                     'origin-id',
                                                     'some-date')

        self.assertEquals(actual_occ, {
            'branch': 'master',
            'target': 'revision-id',
            'target_type': 'revision',
            'origin': 'origin-id',
            'date': 'some-date'})

    @istest
    def svn_date_to_swh_date(self):
        """Date in string should be converted in swh date."""
        actual_ts = converters.svn_date_to_swh_date(
            '2011-05-31T06:04:39.800722Z')

        self.assertEquals(actual_ts,
                          {'timestamp': 1306821879.800722, 'offset': 0})

    @istest
    def svn_date_to_swh_date_epoch(self):
        """Empty date should be EPOCH (timestamp and offset at 0)."""
        # It should return 0, epoch
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          converters.svn_date_to_swh_date(''))
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          converters.svn_date_to_swh_date(None))
