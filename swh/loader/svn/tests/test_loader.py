# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from nose.tools import istest

from swh.model import hashutil
from swh.loader.svn.loader import SWHSvnLoader, SvnLoaderEventful
from swh.loader.svn.loader import SvnLoaderHistoryAltered, SvnLoaderUneventful

from test_base import BaseTestSvnLoader

# Define loaders with no storage
# They'll just accumulate the data in place
# Only for testing purposes.


class TestSvnLoader:
    """Mixin class to inhibit the persistence and keep in memory the data
    sent for storage.

    cf. SWHSvnLoaderNoStorage

    """
    def __init__(self):
        super().__init__()
        self.all_contents = []
        self.all_directories = []
        self.all_revisions = []
        self.all_releases = []
        self.all_occurrences = []
        # Check at each svn revision that the hash tree computation
        # does not diverge
        self.check_revision = 10
        # typed data
        self.objects = {
            'content': self.all_contents,
            'directory': self.all_directories,
            'revision': self.all_revisions,
            'release': self.all_releases,
            'occurrence': self.all_occurrences,
        }

    def _add(self, type, l):
        """Add without duplicates and keeping the insertion order.

        Args:
            type (str): Type of objects concerned by the action
            l ([object]): List of 'type' object

        """
        col = self.objects[type]
        for o in l:
            if o in col:
                continue
            col.extend([o])

    def maybe_load_contents(self, all_contents):
        self._add('content', all_contents)

    def maybe_load_directories(self, all_directories):
        self._add('directory', all_directories)

    def maybe_load_revisions(self, all_revisions):
        self._add('revision', all_revisions)

    def maybe_load_releases(self, releases):
        raise ValueError('If called, the test must break.')

    def maybe_load_occurrences(self, all_occurrences):
        self._add('occurrence', all_occurrences)

    # Override to do nothing at the end
    def close_failure(self):
        pass

    def close_success(self):
        pass

    # Override to only prepare the svn repository
    def prepare(self, *args, **kwargs):
        self.svnrepo = self.get_svn_repo(*args)


class SWHSvnLoaderNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context:
        Load a new svn repository using the swh policy (so no update).

    """
    def swh_previous_revision(self, prev_swh_revision=None):
        """We do not know this repository so no revision.

        """
        return None


class SWHSvnLoaderUpdateNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context:
        Load a known svn repository using the swh policy.
        We can either:
        - do nothing since it does not contain any new commit (so no
          change)
        - either check its history is not altered and update in
          consequence by loading the new revision

    """
    def swh_previous_revision(self, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'id': hashutil.hash_to_bytes(
                '4876cb10aec6f708f7466dddf547567b65f6c39c'),
            'parents': [hashutil.hash_to_bytes(
                'a3a577948fdbda9d1061913b77a1588695eadb41')],
            'directory': hashutil.hash_to_bytes(
                '0deab3023ac59398ae467fc4bff5583008af1ee2'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', '6']
                ]
            }
        }


class SWHSvnLoaderUpdateHistoryAlteredNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context: Load a known svn repository using the swh policy with its
    history altered so we do not update it.

    """
    def swh_previous_revision(self, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            # Changed the revision id's hash to simulate history altered
            'id': hashutil.hash_to_bytes(
                'badbadbadbadf708f7466dddf547567b65f6c39d'),
            'parents': [hashutil.hash_to_bytes(
                'a3a577948fdbda9d1061913b77a1588695eadb41')],
            'directory': hashutil.hash_to_bytes(
                '0deab3023ac59398ae467fc4bff5583008af1ee2'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', b'6']
                ]
            }
        }


class SWHSvnLoaderNewRepositoryITTest(BaseTestSvnLoader):
    def setUp(self):
        super().setUp()

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 2,
        }

        self.loader = SWHSvnLoaderNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Process a new repository with swh policy should be ok.

        """
        # when
        self.loader.process_repository(self.origin_visit)

        # then
        self.assertEquals(len(self.loader.all_revisions), 6)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '4876cb10aec6f708f7466dddf547567b65f6c39c'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '0d7dd5f751cef8fe17e8024f7d6b0e3aac2cfd71': '669a71cce6c424a81ba42b7dc5d560d32252f0ca',  # noqa
            '95edacc8848369d6fb1608e887d6d2474fd5224f': '008ac97a1118560797c50e3392fa1443acdaa349',  # noqa
            'fef26ea45a520071711ba2b9d16a2985ee837021': '3780effbe846a26751a95a8c95c511fb72be15b4',  # noqa
            '3f51abf3b3d466571be0855dfa67e094f9ceff1b': 'ffcca9b09c5827a6b8137322d4339c8055c3ee1e',  # noqa
            'a3a577948fdbda9d1061913b77a1588695eadb41': '7dc52cc04c3b8bd7c085900d60c159f7b846f866',  # noqa
            last_revision:                              '0deab3023ac59398ae467fc4bff5583008af1ee2',  # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderUpdateWithNoChangeITTest(BaseTestSvnLoader):
    def setUp(self):
        super().setUp()

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 3,
        }

        self.loader = SWHSvnLoaderUpdateNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy and no new data should
        be ok.

        """
        # when
        with self.assertRaises(SvnLoaderUneventful):
            self.loader.args = (self.origin_visit,)
            self.loader.process_repository(self.origin_visit)

        # then
        self.assertEquals(len(self.loader.all_revisions), 0)
        self.assertEquals(len(self.loader.all_releases), 0)


class SWHSvnLoaderUpdateWithHistoryAlteredITTest(BaseTestSvnLoader):
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 4,
        }

        self.loader = SWHSvnLoaderUpdateHistoryAlteredNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy and history altered
        should stop and do nothing.

        """
        # when
        with self.assertRaises(SvnLoaderHistoryAltered):
            self.loader.args = (self.origin_visit,)
            self.loader.process_repository(self.origin_visit)

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 news + 1 old
        self.assertEquals(len(self.loader.all_revisions), 0)
        self.assertEquals(len(self.loader.all_releases), 0)


class SWHSvnLoaderUpdateWithChangesITTest(BaseTestSvnLoader):
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 5,
        }

        self.loader = SWHSvnLoaderUpdateNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy and new data should
        yield new revisions and occurrence.

        """
        # when
        self.loader.process_repository(self.origin_visit)

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertEquals(len(self.loader.all_revisions), 5)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '171dc35522bfd17dda4e90a542a0377fb2fc707a'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            last_revision:                              'fd24a76c87a3207428e06612b49860fc78e9f6dc'   # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderUpdateWithUnfinishedLoadingChangesITTest(BaseTestSvnLoader):
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 6
        }

        self.loader = SWHSvnLoaderNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy, the previous run did
        not finish, so this finishes the loading

        """
        previous_unfinished_revision = {
            'id': hashutil.hash_to_bytes(
                '4876cb10aec6f708f7466dddf547567b65f6c39c'),
            'parents': [hashutil.hash_to_bytes(
                'a3a577948fdbda9d1061913b77a1588695eadb41')],
            'directory': hashutil.hash_to_bytes(
                '0deab3023ac59398ae467fc4bff5583008af1ee2'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', '6']
                ]
            }
        }
        # when
        self.loader.process_repository(
            self.origin_visit,
            last_known_swh_revision=previous_unfinished_revision)

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertEquals(len(self.loader.all_revisions), 5)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '171dc35522bfd17dda4e90a542a0377fb2fc707a'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            last_revision:                              'fd24a76c87a3207428e06612b49860fc78e9f6dc'   # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderUpdateWithUnfinishedLoadingChangesButOccurrenceDoneITTest(
        BaseTestSvnLoader):
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 9,
        }

        self.loader = SWHSvnLoaderUpdateNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """known repository, swh policy, unfinished revision is less recent
        than occurrence, we start from last occurrence.

        """
        previous_unfinished_revision = {
            'id': hashutil.hash_to_bytes(
                'a3a577948fdbda9d1061913b77a1588695eadb41'),
            'parents': [hashutil.hash_to_bytes(
                '3f51abf3b3d466571be0855dfa67e094f9ceff1b')],
            'directory': hashutil.hash_to_bytes(
                '7dc52cc04c3b8bd7c085900d60c159f7b846f866'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', '5']
                ]
            }
        }

        # when
        self.loader.process_repository(
            self.origin_visit,
            last_known_swh_revision=previous_unfinished_revision)

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertEquals(len(self.loader.all_revisions), 5)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '171dc35522bfd17dda4e90a542a0377fb2fc707a'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            last_revision:                              'fd24a76c87a3207428e06612b49860fc78e9f6dc'   # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderUpdateLessRecentNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context:
        Load a known svn repository using the swh policy.
        The last occurrence seen is less recent than a previous
        unfinished crawl.

    """
    def swh_previous_revision(self, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'id': hashutil.hash_to_bytes(
                'a3a577948fdbda9d1061913b77a1588695eadb41'),
            'parents': [hashutil.hash_to_bytes(
                '3f51abf3b3d466571be0855dfa67e094f9ceff1b')],
            'directory': hashutil.hash_to_bytes(
                '7dc52cc04c3b8bd7c085900d60c159f7b846f866'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', '5']
                ]
            }
        }


class SWHSvnLoaderUnfinishedLoadingChangesSinceLastOccurrenceITTest(
        BaseTestSvnLoader):
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 1,
        }

        self.loader = SWHSvnLoaderUpdateLessRecentNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """known repository, swh policy, unfinished revision is less recent
        than occurrence, we start from last occurrence.

        """
        previous_unfinished_revision = {
            'id': hashutil.hash_to_bytes(
                '4876cb10aec6f708f7466dddf547567b65f6c39c'),
            'parents': [hashutil.hash_to_bytes(
                'a3a577948fdbda9d1061913b77a1588695eadb41')],
            'directory': hashutil.hash_to_bytes(
                '0deab3023ac59398ae467fc4bff5583008af1ee2'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', '6']
                ]
            }
        }
        # when
        self.loader.process_repository(
            self.origin_visit,
            last_known_swh_revision=previous_unfinished_revision)

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertEquals(len(self.loader.all_revisions), 5)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '171dc35522bfd17dda4e90a542a0377fb2fc707a'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            last_revision:                              'fd24a76c87a3207428e06612b49860fc78e9f6dc'   # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderUpdateAndTestCornerCasesAboutEolITTest(BaseTestSvnLoader):
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-eol-corner-cases.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 1,
        }

        self.loader = SWHSvnLoaderUpdateLessRecentNoStorage()
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """EOL corner cases and update.

        """
        previous_unfinished_revision = {
            'id': hashutil.hash_to_bytes(
                '171dc35522bfd17dda4e90a542a0377fb2fc707a'),
            'parents': [hashutil.hash_to_bytes(
                '902f29b4323a9b9de3af6d28e72dd581e76d9397')],
            'directory': hashutil.hash_to_bytes(
                'fd24a76c87a3207428e06612b49860fc78e9f6dc'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', '11']
                ]
            }
        }
        # when
        self.loader.process_repository(
            self.origin_visit,
            last_known_swh_revision=previous_unfinished_revision)

        # then
        # we got the previous run's last revision (rev 11)
        # so 8 new
        self.assertEquals(len(self.loader.all_revisions), 8)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '0148ae3eaa520b73a50802c59f3f416b7a36cf8c'

        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '027e8769f4786597436ab94a91f85527d04a6cbb': '2d9ca72c6afec6284fb01e459588cbb007017c8c',  # noqa
            '4474d96018877742d9697d5c76666c9693353bfc': 'ab111577e0ab39e4a157c476072af48f2641d93f',  # noqa
            '97ad21eab92961e2a22ca0285f09c6d1e9a7ffbc': 'ab111577e0ab39e4a157c476072af48f2641d93f',  # noqa
            'd04ea8afcee6205cc8384c091bfc578931c169fd': 'b0a648b02e55a4dce356ac35187a058f89694ec7',  # noqa
            'ded78810401fd354ffe894aa4a1e5c7d30a645d1': 'b0a648b02e55a4dce356ac35187a058f89694ec7',  # noqa
            '4ee95e39358712f53c4fc720da3fafee9249ed19': 'c3c98df624733fef4e592bef983f93e2ed02b179',  # noqa
            'ffa901b69ca0f46a2261f42948838d19709cb9f8': 'c3c98df624733fef4e592bef983f93e2ed02b179',  # noqa
            last_revision:                              '844d4646d6c2b4f3a3b2b22ab0ee38c7df07bab2',  # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderExternalIdCornerCaseITTest(BaseTestSvnLoader):
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-external-id.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 1,
        }

        self.loader = SWHSvnLoaderNoStorage()
        # override revision-block size
        self.loader.config['revision_packet_size'] = 3
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Repository with svn:externals propery, will stop raising an error

        """
        previous_unfinished_revision = None

        # when
        with self.assertRaises(SvnLoaderEventful) as exc:
            self.loader.process_repository(
                self.origin_visit,
                last_known_swh_revision=previous_unfinished_revision)

        actual_raised_revision = exc.exception.swh_revision

        # then repositories holds 21 revisions, but the last commit
        # one holds an 'svn:externals' property which will make the
        # loader-svn stops. This will then stop at the 6th iterations
        # of 3-revision block size, so only 18 revisions will be
        # flushed
        self.assertEquals(len(self.loader.all_revisions), 18)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = 'ffa901b69ca0f46a2261f42948838d19709cb9f8'

        expected_revisions = {
            # revision hash | directory hash
            '0d7dd5f751cef8fe17e8024f7d6b0e3aac2cfd71': '669a71cce6c424a81ba42b7dc5d560d32252f0ca',  # noqa
            '95edacc8848369d6fb1608e887d6d2474fd5224f': '008ac97a1118560797c50e3392fa1443acdaa349',  # noqa
            'fef26ea45a520071711ba2b9d16a2985ee837021': '3780effbe846a26751a95a8c95c511fb72be15b4',  # noqa
            '3f51abf3b3d466571be0855dfa67e094f9ceff1b': 'ffcca9b09c5827a6b8137322d4339c8055c3ee1e',  # noqa
            'a3a577948fdbda9d1061913b77a1588695eadb41': '7dc52cc04c3b8bd7c085900d60c159f7b846f866',  # noqa
            '4876cb10aec6f708f7466dddf547567b65f6c39c': '0deab3023ac59398ae467fc4bff5583008af1ee2',  # noqa
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            '171dc35522bfd17dda4e90a542a0377fb2fc707a': 'fd24a76c87a3207428e06612b49860fc78e9f6dc',  # noqa
            '027e8769f4786597436ab94a91f85527d04a6cbb': '2d9ca72c6afec6284fb01e459588cbb007017c8c',  # noqa
            '4474d96018877742d9697d5c76666c9693353bfc': 'ab111577e0ab39e4a157c476072af48f2641d93f',  # noqa
            '97ad21eab92961e2a22ca0285f09c6d1e9a7ffbc': 'ab111577e0ab39e4a157c476072af48f2641d93f',  # noqa
            'd04ea8afcee6205cc8384c091bfc578931c169fd': 'b0a648b02e55a4dce356ac35187a058f89694ec7',  # noqa
            'ded78810401fd354ffe894aa4a1e5c7d30a645d1': 'b0a648b02e55a4dce356ac35187a058f89694ec7',  # noqa
            '4ee95e39358712f53c4fc720da3fafee9249ed19': 'c3c98df624733fef4e592bef983f93e2ed02b179',  # noqa
            last_revision                             : 'c3c98df624733fef4e592bef983f93e2ed02b179',  # noqa
        }

        # The last revision being the one used later to start back from
        self.assertEquals(hashutil.hash_to_hex(actual_raised_revision['id']),
                          last_revision)

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderLinkFileAndFolderWithSameNameITTest(BaseTestSvnLoader):
    def setUp(self):
        # edge cases:
        # - first create a file and commit it.
        #   Remove it, then add folder holding the same name, commit.
        # - do the same scenario with symbolic link (instead of file)
        super().setUp(
            archive_name='pkg-gourmet-with-edge-case-links-and-files.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 1,
        }

        self.loader = SWHSvnLoaderNoStorage()
        # override revision-block size
        self.loader.config['revision_packet_size'] = 3
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """File/Link destroyed prior to folder with same name creation should be ok

        """
        previous_unfinished_revision = None

        # when
        self.loader.process_repository(
            self.origin_visit,
            last_known_swh_revision=previous_unfinished_revision)

        # then repositories holds 14 revisions, but the last commit
        self.assertEquals(len(self.loader.all_revisions), 19)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '3f43af2578fccf18b0d4198e48563da7929dc608'
        expected_revisions = {
            # revision hash | directory hash
            '0d7dd5f751cef8fe17e8024f7d6b0e3aac2cfd71': '669a71cce6c424a81ba42b7dc5d560d32252f0ca',  # noqa
            '95edacc8848369d6fb1608e887d6d2474fd5224f': '008ac97a1118560797c50e3392fa1443acdaa349',  # noqa
            'fef26ea45a520071711ba2b9d16a2985ee837021': '3780effbe846a26751a95a8c95c511fb72be15b4',  # noqa
            '3f51abf3b3d466571be0855dfa67e094f9ceff1b': 'ffcca9b09c5827a6b8137322d4339c8055c3ee1e',  # noqa
            'a3a577948fdbda9d1061913b77a1588695eadb41': '7dc52cc04c3b8bd7c085900d60c159f7b846f866',  # noqa
            '4876cb10aec6f708f7466dddf547567b65f6c39c': '0deab3023ac59398ae467fc4bff5583008af1ee2',  # noqa
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            '171dc35522bfd17dda4e90a542a0377fb2fc707a': 'fd24a76c87a3207428e06612b49860fc78e9f6dc',  # noqa
            '9231f9a98a9051a0cd34231cddd4e11773f8348e': '6c07f4f4ac780eaf99a247fbfd0897533598dd36',  # noqa
            'c309bd3b57796696d6655ab3ab0b438fdd2d8201': 'fd24a76c87a3207428e06612b49860fc78e9f6dc',  # noqa
            'bb78300cc1ac9119eb6fffa9e9fa04a7f9340b11': 'ee995a0d85f6917c75bcee3aa448bea7726b265d',  # noqa
            'f2e01111329f84580dc3febb1fd45515692c5886': 'e2baec7b6a5543758e9c73695bc847db0a4f7941',  # noqa
            '1a0f70c34e211f073e1be3435ecf6f0dd7700267': 'e7536e721fa806c19971b749c091c144b2f2b88e',  # noqa
            '0c612a23d293cc3100496a54ae4ad13d750efe4c': '2123d12749294bbfb54e73f9d73fac658aabb266',  # noqa
            '69a53d972e2f863acbbbda546d9da96287af6a88': '13896cb96ec004140ce5be8852fee8c29830d9c7',  # noqa
            last_revision:                              '6b1e0243768ff9ac060064b2eeade77e764ffc82',  # noqa
        }

        # The last revision being the one used later to start back from
        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


class SWHSvnLoaderWrongLinkCasesITTest(BaseTestSvnLoader):
    def setUp(self):
        # edge cases:
        # - wrong symbolic link
        # - wrong symbolic link with empty space names
        super().setUp(
            archive_name='pkg-gourmet-with-wrong-link-cases.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.origin_visit = {
            'origin': self.origin['id'],
            'visit': 1,
        }

        self.loader = SWHSvnLoaderNoStorage()
        # override revision-block size
        self.loader.config['revision_packet_size'] = 3
        self.loader.prepare(
            self.svn_mirror_url, self.destination_path, self.origin)

    @istest
    def process_repository(self):
        """Wrong link or empty space-named link should be ok

        """
        previous_unfinished_revision = None

        # when
        self.loader.process_repository(
            self.origin_visit,
            last_known_swh_revision=previous_unfinished_revision)

        # then repositories holds 14 revisions, but the last commit
        self.assertEquals(len(self.loader.all_revisions), 21)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = 'cf30d3bb9d5967d0a2bbeacc405f10a5dd9b138a'

        expected_revisions = {
            # revision hash | directory hash
            '0d7dd5f751cef8fe17e8024f7d6b0e3aac2cfd71': '669a71cce6c424a81ba42b7dc5d560d32252f0ca',  # noqa
            '95edacc8848369d6fb1608e887d6d2474fd5224f': '008ac97a1118560797c50e3392fa1443acdaa349',  # noqa
            'fef26ea45a520071711ba2b9d16a2985ee837021': '3780effbe846a26751a95a8c95c511fb72be15b4',  # noqa
            '3f51abf3b3d466571be0855dfa67e094f9ceff1b': 'ffcca9b09c5827a6b8137322d4339c8055c3ee1e',  # noqa
            'a3a577948fdbda9d1061913b77a1588695eadb41': '7dc52cc04c3b8bd7c085900d60c159f7b846f866',  # noqa
            '4876cb10aec6f708f7466dddf547567b65f6c39c': '0deab3023ac59398ae467fc4bff5583008af1ee2',  # noqa
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            '38d81702cb28db4f1a6821e64321e5825d1f7fd6': '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
            '99c27ebbd43feca179ac0e895af131d8314cafe1': '3397ca7f709639cbd36b18a0d1b70bce80018c45',  # noqa
            '902f29b4323a9b9de3af6d28e72dd581e76d9397': 'c4e12483f0a13e6851459295a4ae735eb4e4b5c4',  # noqa
            '171dc35522bfd17dda4e90a542a0377fb2fc707a': 'fd24a76c87a3207428e06612b49860fc78e9f6dc',  # noqa
            '9231f9a98a9051a0cd34231cddd4e11773f8348e': '6c07f4f4ac780eaf99a247fbfd0897533598dd36',  # noqa
            'c309bd3b57796696d6655ab3ab0b438fdd2d8201': 'fd24a76c87a3207428e06612b49860fc78e9f6dc',  # noqa
            'bb78300cc1ac9119eb6fffa9e9fa04a7f9340b11': 'ee995a0d85f6917c75bcee3aa448bea7726b265d',  # noqa
            'f2e01111329f84580dc3febb1fd45515692c5886': 'e2baec7b6a5543758e9c73695bc847db0a4f7941',  # noqa
            '1a0f70c34e211f073e1be3435ecf6f0dd7700267': 'e7536e721fa806c19971b749c091c144b2f2b88e',  # noqa
            '0c612a23d293cc3100496a54ae4ad13d750efe4c': '2123d12749294bbfb54e73f9d73fac658aabb266',  # noqa
            '69a53d972e2f863acbbbda546d9da96287af6a88': '13896cb96ec004140ce5be8852fee8c29830d9c7',  # noqa
            '3f43af2578fccf18b0d4198e48563da7929dc608': '6b1e0243768ff9ac060064b2eeade77e764ffc82',  # noqa
            '4ab5fc264732cd474d2e695c5ac66e4933bdad74': '9a1f5e3961db89422250ce6c1441476f40d65205',  # noqa
            last_revision:                              'd853d9628f6f0008d324fed27dadad00ce48bc62',  # noqa
        }

        # The last revision being the one used later to start back from
        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)
