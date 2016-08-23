# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import converters


class TestAuthorGitSvnConverters(unittest.TestCase):
    @istest
    def svn_author_to_gitsvn_person(self):
        """The author should have name, email and fullname filled.

        """
        actual_person = converters.svn_author_to_gitsvn_person(
            'tony <ynot@dagobah>',
            repo_uuid=None)
        self.assertEquals(actual_person, {
            'fullname': b'tony <ynot@dagobah>',
            'name': b'tony',
            'email': b'ynot@dagobah',
        })

    @istest
    def svn_author_to_gitsvn_person_no_email(self):
        """The author should see his/her email filled with author@<repo-uuid>.

        """
        actual_person = converters.svn_author_to_gitsvn_person(
            'tony',
            repo_uuid=b'some-uuid')
        self.assertEquals(actual_person, {
            'fullname': b'tony <tony@some-uuid>',
            'name': b'tony',
            'email': b'tony@some-uuid',
        })

    @istest
    def svn_author_to_gitsvn_person_empty_person(self):
        """The empty person should see name, fullname and email filled.

        """
        actual_person = converters.svn_author_to_gitsvn_person(
            '',
            repo_uuid=b'some-uuid')
        self.assertEqual(actual_person, {
            'fullname': b'(no author) <(no author)@some-uuid>',
            'name': b'(no author)',
            'email': b'(no author)@some-uuid'
        })


class TestAuthorSWHConverters(unittest.TestCase):
    @istest
    def svn_author_to_swh_person(self):
        """The author should have name, email and fullname filled.

        """
        actual_person = converters.svn_author_to_swh_person(
            'tony <ynot@dagobah>')
        self.assertEquals(actual_person, {
            'fullname': b'tony <ynot@dagobah>',
            'name': b'tony',
            'email': b'ynot@dagobah',
        })

    @istest
    def svn_author_to_swh_person_no_email(self):
        """The author and fullname should be the same as the input (author).

        """
        actual_person = converters.svn_author_to_swh_person('tony')
        self.assertEquals(actual_person, {
            'fullname': b'tony',
            'name': b'tony',
            'email': None,
        })

    @istest
    def svn_author_to_swh_person_empty_person(self):
        """Empty person has only its fullname filled with the empty
        byte-string.

        """
        actual_person = converters.svn_author_to_swh_person('')
        self.assertEqual(actual_person, {
            'fullname': b'',
            'name': None,
            'email': None,
        })


class TestSWHRevisionConverters(unittest.TestCase):
    @istest
    def build_swh_revision_default(self):
        """This should build the swh revision with the swh revision's extra
        headers about the repository.

        """
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


class TestGitSvnRevisionConverters(unittest.TestCase):
    @istest
    def build_gitsvn_swh_revision_default(self):
        """This should build the swh revision without the swh revision's extra
        headers about the repository.

        """
        actual_swh_revision = converters.build_gitsvn_swh_revision(
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
            'metadata': None,
            'parents': ['123'],
        })


class TestSWHOccurrence(unittest.TestCase):
    @istest
    def build_swh_occurrence(self):
        actual_occ = converters.build_swh_occurrence('revision-id',
                                                     'origin-id',
                                                     visit=10)

        self.assertEquals(actual_occ, {
            'branch': 'master',
            'target': 'revision-id',
            'target_type': 'revision',
            'origin': 'origin-id',
            'visit': 10
        })


class ConvertSWHDate(unittest.TestCase):
    @istest
    def svn_date_to_swh_date(self):
        """The timestamp should not be tampered with and include the
        decimals.

        """
        self.assertEquals(
            converters.svn_date_to_swh_date('2011-05-31T06:04:39.500900Z'),
            {
                'timestamp': 1306821879.5009,
                'offset': 0
            })

        self.assertEquals(
            converters.svn_date_to_swh_date('2011-05-31T06:04:39.800722Z'),
            {
                'timestamp': 1306821879.800722,
                'offset': 0
            })

    @istest
    def svn_date_to_swh_date_epoch(self):
        """Empty date should be EPOCH (timestamp and offset at 0)."""
        # It should return 0, epoch
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          converters.svn_date_to_swh_date(''))
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          converters.svn_date_to_swh_date(None))


class ConvertGitSvnDate(unittest.TestCase):
    @istest
    def svn_date_to_gitsvn_date(self):
        """The timestamp should be truncated to be an integer."""
        actual_ts = converters.svn_date_to_gitsvn_date(
            '2011-05-31T06:04:39.800722Z')

        self.assertEquals(actual_ts,
                          {'timestamp': 1306821879, 'offset': 0})

    @istest
    def svn_date_to_gitsvn_date_epoch(self):
        """Empty date should be EPOCH (timestamp and offset at 0)."""
        # It should return 0, epoch
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          converters.svn_date_to_gitsvn_date(''))
        self.assertEquals({'timestamp': 0, 'offset': 0},
                          converters.svn_date_to_gitsvn_date(None))


class ConvertSWHRevision(unittest.TestCase):
    @istest
    def loader_to_scheduler_revision(self):
        actual_rev = converters.loader_to_scheduler_revision({
            'parents': [b'e\n\xbe\xe9\xc0\x87y\xfeG\xf7\xcfG\x82h\xa8i\xe8\xfe\xe2\x13'],  # noqa
            'id': b'\xedd\x92w\xab\xb2\x16,\xea*\x90O8\x0f\x96/\xfb\xd4\x16`',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', b'bc7d6c17-68a5-4917-9c54-c565d7424229'],
                    ['svn_revision', b'4']
                ]
            }
        })

        self.assertEquals(actual_rev, {
            'id': 'ed649277abb2162cea2a904f380f962ffbd41660',
            'parents': ['650abee9c08779fe47f7cf478268a869e8fee213'],
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', 'bc7d6c17-68a5-4917-9c54-c565d7424229'],
                    ['svn_revision', '4']
                ]
            }
        })

    @istest
    def loader_to_scheduler_revision_none(self):
        self.assertIsNone(converters.loader_to_scheduler_revision(None))

    @istest
    def scheduler_to_loader_revision(self):
        actual_rev = converters.scheduler_to_loader_revision({
            'id': 'ed649277abb2162cea2a904f380f962ffbd41660',
            'parents': ['650abee9c08779fe47f7cf478268a869e8fee213'],
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', 'bc7d6c17-68a5-4917-9c54-c565d7424229'],
                    ['svn_revision', '4']
                ]
            }
        })

        self.assertEquals(actual_rev, {
            'parents': [b'e\n\xbe\xe9\xc0\x87y\xfeG\xf7\xcfG\x82h\xa8i\xe8\xfe\xe2\x13'],  # noqa
            'id': b'\xedd\x92w\xab\xb2\x16,\xea*\x90O8\x0f\x96/\xfb\xd4\x16`',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', 'bc7d6c17-68a5-4917-9c54-c565d7424229'],
                    ['svn_revision', '4']
                ]
            }
        })

    @istest
    def scheduler_to_loader_revision_none(self):
        self.assertIsNone(converters.scheduler_to_loader_revision(None))
