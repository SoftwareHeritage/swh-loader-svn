# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import converters


class TestConverters(unittest.TestCase):
    @istest
    def svn_author_to_person_as_bytes(self):
        actual_person1 = converters.svn_author_to_person(
            b'tony <ynot@dagobah>')
        self.assertEquals(actual_person1, {
            'fullname': b'tony <ynot@dagobah>',
            'name': b'tony <ynot@dagobah>',
            'email': None,
        })

    @istest
    def svn_author_to_person_as_str(self):
        # should not happen - input is bytes but nothing prevents it
        actual_person1 = converters.svn_author_to_person('tony <tony@dagobah>')
        self.assertEquals(actual_person1, {
            'fullname': 'tony <tony@dagobah>',
            'name': 'tony <tony@dagobah>',
            'email': None,
        })

    @istest
    def svn_author_to_person_None(self):
        # should not happen - nothing prevents it though
        actual_person2 = converters.svn_author_to_person(None)
        self.assertEquals(actual_person2, {
            'fullname': None,
            'name': None,
            'email': None,
        })

    @istest
    def build_swh_revision_default(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid='uuid',
            dir_id='dir-id',
            commit={'author_name': b'theo',
                    'message': b'commit message',
                    'author_date': '2009-04-18 06:55:53 +0200'},
            rev=10,
            parents=['123'])

        self.assertEquals(actual_swh_revision, {
            'date': {'timestamp': '2009-04-18 06:55:53 +0200', 'offset': 0},
            'committer_date': {'timestamp': '2009-04-18 06:55:53 +0200',
                               'offset': 0},
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'commit message',
            'author': {'name': b'theo', 'email': None, 'fullname': b'theo'},
            'committer': {'name': b'theo', 'email': None, 'fullname': b'theo'},
            'synthetic': True,
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', 'uuid'],
                    ['svn_revision', 10],
                ]
            },
            'parents': ['123'],
        })

    @istest
    def build_swh_revision_no_extra_headers(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid='uuid',
            dir_id='dir-id',
            commit={'author_name': b'theo',
                    'message': b'commit message',
                    'author_date': '2009-04-18 06:55:53 +0200'},
            rev=10,
            parents=['123'],
            with_extra_headers=False)

        self.assertEquals(actual_swh_revision, {
            'date': {'timestamp': '2009-04-18 06:55:53 +0200', 'offset': 0},
            'committer_date': {'timestamp': '2009-04-18 06:55:53 +0200',
                               'offset': 0},
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'commit message',
            'author': {'name': b'theo', 'email': None, 'fullname': b'theo'},
            'committer': {'name': b'theo', 'email': None, 'fullname': b'theo'},
            'synthetic': True,
            'metadata': None,
            'parents': ['123'],
        })

    @istest
    def build_swh_revision_empty_data(self):
        actual_swh_revision = converters.build_swh_revision(
            repo_uuid='uuid',
            dir_id='dir-id',
            commit={'author_name': b'',
                    'message': b'',
                    'author_date': '2009-04-10 06:55:53'},
            rev=8,
            parents=[])

        self.assertEquals(actual_swh_revision, {
            'date': {'timestamp': '2009-04-10 06:55:53', 'offset': 0},
            'committer_date': {'timestamp': '2009-04-10 06:55:53',
                               'offset': 0},
            'type': 'svn',
            'directory': 'dir-id',
            'message': b'',
            'author': {'name': b'', 'email': None, 'fullname': b''},
            'committer': {'name': b'', 'email': None, 'fullname': b''},
            'synthetic': True,
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', 'uuid'],
                    ['svn_revision', 8],
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
