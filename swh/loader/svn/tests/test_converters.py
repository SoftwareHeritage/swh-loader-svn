# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import converters


class TestConverters(unittest.TestCase):
    @istest
    def uid_to_person(self):
        actual_person1 = converters.uid_to_person('tony <ynot@dagobah>',
                                                  encode=False)
        self.assertEquals(actual_person1, {
            'name': 'tony',
            'email': 'ynot@dagobah'
        })

        actual_person2 = converters.uid_to_person('ardumont <ard@dagobah>',
                                                  encode=True)
        self.assertEquals(actual_person2, {
            'name': b'ardumont',
            'email': b'ard@dagobah'
        })

        actual_person3 = converters.uid_to_person('someone')
        self.assertEquals(actual_person3, {
            'name': b'someone',
            'email': b''
        })

    @istest
    def build_swh_revision(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid='uuid',
            dir_id='dir-id',
            commit={'author_name': 'theo',
                    'message': 'commit message',
                    'author_date': '2009-04-18 06:55:53 +0200'},
            rev=10,
            parents=['123'])

        self.assertEquals(actual_swh_revision, {
            'date': {'timestamp': '2009-04-18 06:55:53 +0200', 'offset': 0},
            'committer_date': {'timestamp': '2009-04-18 06:55:53 +0200', 'offset': 0},
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'commit message',
            'author': {'name': b'theo', 'email': b''},
            'committer': {'name': b'theo', 'email': b''},
            'synthetic': True,
            'metadata': {
                'extra_headers': {
                    'svn_repo_uuid': 'uuid',
                    'svn_revision': 10,
                }
            },
            'parents': ['123'],
    })

    @istest
    def build_swh_revision_empty_data(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid='uuid',
            dir_id='dir-id',
            commit={'author_name': None,
                    'message': None,
                    'author_date': '2009-04-10 06:55:53'},
            rev=8,
            parents=[])

        self.assertEquals(actual_swh_revision, {
            'date': {'timestamp': '2009-04-10 06:55:53', 'offset': 0},
            'committer_date': {'timestamp': '2009-04-10 06:55:53', 'offset': 0},
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'',
            'author': {'name': b'', 'email': b''},
            'committer': {'name': b'', 'email': b''},
            'synthetic': True,
            'metadata': {
                'extra_headers': {
                    'svn_repo_uuid': 'uuid',
                    'svn_revision': 8,
                }
            },
            'parents': [],
    })

    @istest
    def build_swh_occurrence(self):
        actual_occ = converters.build_swh_occurrence('revision-id', 'origin-id', 'some-date')

        self.assertEquals(actual_occ, {
            'branch': 'master',
            'target': 'revision-id',
            'target_type': 'revision',
            'origin': 'origin-id',
            'date': 'some-date'})
