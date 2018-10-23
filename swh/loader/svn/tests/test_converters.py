# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.loader.svn import converters


class TestAuthorGitSvnConverters(unittest.TestCase):
    def test_svn_author_to_gitsvn_person(self):
        """The author should have name, email and fullname filled.

        """
        actual_person = converters.svn_author_to_gitsvn_person(
            'tony <ynot@dagobah>',
            repo_uuid=None)
        self.assertEqual(actual_person, {
            'fullname': b'tony <ynot@dagobah>',
            'name': b'tony',
            'email': b'ynot@dagobah',
        })

    def test_svn_author_to_gitsvn_person_no_email(self):
        """The author should see his/her email filled with author@<repo-uuid>.

        """
        actual_person = converters.svn_author_to_gitsvn_person(
            'tony',
            repo_uuid=b'some-uuid')
        self.assertEqual(actual_person, {
            'fullname': b'tony <tony@some-uuid>',
            'name': b'tony',
            'email': b'tony@some-uuid',
        })

    def test_svn_author_to_gitsvn_person_empty_person(self):
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


class TestAuthorConverters(unittest.TestCase):
    def test_svn_author_to_swh_person(self):
        """The author should have name, email and fullname filled.

        """
        actual_person = converters.svn_author_to_swh_person(
            'tony <ynot@dagobah>')
        self.assertEqual(actual_person, {
            'fullname': b'tony <ynot@dagobah>',
            'name': b'tony',
            'email': b'ynot@dagobah',
        })

    def test_svn_author_to_swh_person_no_email(self):
        """The author and fullname should be the same as the input (author).

        """
        actual_person = converters.svn_author_to_swh_person('tony')
        self.assertEqual(actual_person, {
            'fullname': b'tony',
            'name': b'tony',
            'email': None,
        })

    def test_svn_author_to_swh_person_empty_person(self):
        """Empty person has only its fullname filled with the empty
        byte-string.

        """
        actual_person = converters.svn_author_to_swh_person('')
        self.assertEqual(actual_person, {
            'fullname': b'',
            'name': None,
            'email': None,
        })


class TestRevisionConverters(unittest.TestCase):
    def test_build_swh_revision_default(self):
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
                    'timestamp': {
                        'seconds': 1088108379,
                        'microseconds': 0,
                    },
                    'offset': 0
                }
            },
            rev=10,
            parents=['123'])

        date = {
            'timestamp': {
                'seconds': 1088108379,
                'microseconds': 0,
            },
            'offset': 0,
        }

        self.assertEqual(actual_swh_revision, {
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
    def test_build_gitsvn_swh_revision_default(self):
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
                    'timestamp': {
                        'seconds': 1088108379,
                        'microseconds': 0,
                    },
                    'offset': 0
                }
            },
            rev=10,
            parents=['123'])

        date = {
            'timestamp': {
                'seconds': 1088108379,
                'microseconds': 0,
            },
            'offset': 0,
        }

        self.assertEqual(actual_swh_revision, {
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


class ConvertDate(unittest.TestCase):
    def test_svn_date_to_swh_date(self):
        """The timestamp should not be tampered with and include the
        decimals.

        """
        self.assertEqual(
            converters.svn_date_to_swh_date('2011-05-31T06:04:39.500900Z'), {
                'timestamp': {
                    'seconds': 1306821879,
                    'microseconds': 500900,
                },
                'offset': 0
            })

        self.assertEqual(
            converters.svn_date_to_swh_date('2011-05-31T06:04:39.800722Z'),
            {
                'timestamp': {
                    'seconds': 1306821879,
                    'microseconds': 800722,
                },
                'offset': 0
            })

    def test_svn_date_to_swh_date_epoch(self):
        """Empty date should be EPOCH (timestamp and offset at 0)."""
        # It should return 0, epoch
        self.assertEqual({
            'timestamp': {
                'seconds': 0,
                'microseconds': 0,
            },
            'offset': 0,
        }, converters.svn_date_to_swh_date(''))
        self.assertEqual({
            'timestamp': {
                'seconds': 0,
                'microseconds': 0,
            }, 'offset': 0,
        }, converters.svn_date_to_swh_date(None))


class ConvertGitSvnDate(unittest.TestCase):
    def test_svn_date_to_gitsvn_date(self):
        """The timestamp should be truncated to be an integer."""
        actual_ts = converters.svn_date_to_gitsvn_date(
            '2011-05-31T06:04:39.800722Z')

        self.assertEqual(actual_ts, {
            'timestamp': {
                'seconds': 1306821879,
                'microseconds': 0,
            },
            'offset': 0,
        })

    def test_svn_date_to_gitsvn_date_epoch(self):
        """Empty date should be EPOCH (timestamp and offset at 0)."""
        # It should return 0, epoch
        self.assertEqual({
            'timestamp': {
                'seconds': 0,
                'microseconds': 0,
            },
            'offset': 0,
        }, converters.svn_date_to_gitsvn_date(''))
        self.assertEqual({
            'timestamp': {
                'seconds': 0,
                'microseconds': 0,
            },
            'offset': 0,
        }, converters.svn_date_to_gitsvn_date(None))
