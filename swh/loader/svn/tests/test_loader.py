# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from nose.tools import istest
from test_base import BaseTestSvnLoader
from unittest import TestCase

from swh.model import hashutil

from swh.loader.svn.loader import build_swh_snapshot, DEFAULT_BRANCH
from swh.loader.svn.loader import SWHSvnLoader
from swh.loader.svn.exception import SvnLoaderEventful, SvnLoaderUneventful
from swh.loader.svn.exception import SvnLoaderHistoryAltered


class TestSWHSnapshot(TestCase):
    @istest
    def build_swh_snapshot(self):
        actual_snap = build_swh_snapshot('revision-id')

        self.assertEquals(actual_snap, {
            'id': None,
            'branches': {
                DEFAULT_BRANCH: {
                    'target': 'revision-id',
                    'target_type': 'revision',
                }
            }
        })


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
        self.all_snapshots = []

        # Check at each svn revision that the hash tree computation
        # does not diverge
        self.check_revision = 10
        # typed data
        self.objects = {
            'content': self.all_contents,
            'directory': self.all_directories,
            'revision': self.all_revisions,
            'release': self.all_releases,
            'snapshot': self.all_snapshots,
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

    def maybe_load_snapshot(self, snapshot):
        self._add('snapshot', [snapshot])

    def send_origin(self, origin):
        return 1

    # Override to do nothing at the end
    def close_failure(self):
        pass

    def close_success(self):
        pass

    def pre_cleanup(self):
        pass


class SvnLoaderNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context:
        Load a new svn repository using the swh policy (so no update).

    """
    def swh_latest_snapshot_revision(self, origin_id, prev_swh_revision=None):
        """We do not know this repository so no revision.

        """
        return {}


class SvnLoaderUpdateNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context:
        Load a known svn repository using the swh policy.
        We can either:
        - do nothing since it does not contain any new commit (so no
          change)
        - either check its history is not altered and update in
          consequence by loading the new revision

    """
    def swh_latest_snapshot_revision(self, origin_id, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'snapshot': 'something',  # need a snapshot of sort
            'revision': {
                'id': hashutil.hash_to_bytes(
                    '4876cb10aec6f708f7466dddf547567b65f6c39c'),
                'parents': [hashutil.hash_to_bytes(
                    'a3a577948fdbda9d1061913b77a1588695eadb41')],
                'directory': hashutil.hash_to_bytes(
                    '0deab3023ac59398ae467fc4bff5583008af1ee2'),
                'target_type': 'revision',
                'metadata': {
                    'extra_headers': [
                        ['svn_repo_uuid',
                         '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                        ['svn_revision', '6']
                    ]
                }
            }
        }


class SvnLoaderUpdateHistoryAlteredNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context: Load a known svn repository using the swh policy with its
    history altered so we do not update it.

    """
    def swh_latest_snapshot_revision(self, origin_id, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'snapshot': None,
            'revision': {
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
                        ['svn_repo_uuid',
                         '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                        ['svn_revision', b'6']
                    ]
                }
            }
        }


class SvnLoaderITest1(BaseTestSvnLoader):
    """Load an unknown svn repository results in new data.

    """
    def setUp(self):
        super().setUp()
        self.loader = SvnLoaderNoStorage()

    @istest
    def load(self):
        """Load a new repository results in new swh object and snapshot

        """
        # when
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path)

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

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITest2(BaseTestSvnLoader):
    """Load a visited repository with no new change results in no data
       change.

    """
    def setUp(self):
        super().setUp()
        self.loader = SvnLoaderUpdateNoStorage()

    @istest
    def load(self):
        """Load a repository without new changes results in same snapshot

        """
        # when
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path)

        # then

        self.assertEquals(len(self.loader.all_contents), 0)
        self.assertEquals(len(self.loader.all_directories), 0)
        self.assertEquals(len(self.loader.all_revisions), 0)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'uneventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITest3(BaseTestSvnLoader):
    """In this scenario, the dump has been tampered with to modify the
       commit log.  This results in a hash divergence which is
       detected at startup.

       In effect, that stops the loading and do nothing.

    """
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')
        self.loader = SvnLoaderUpdateHistoryAlteredNoStorage()

    @istest
    def load(self):
        """Load known repository with history altered should do nothing

        """
        # when
        self.loader.load(svn_url=self.svn_mirror_url,
                         destination_path=self.destination_path)

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 news + 1 old
        self.assertEquals(len(self.loader.all_contents), 0)
        self.assertEquals(len(self.loader.all_directories), 0)
        self.assertEquals(len(self.loader.all_revisions), 0)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_snapshots), 0)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'uneventful'})
        self.assertEqual(self.loader.visit_status(), 'partial')


class SvnLoaderITest4(BaseTestSvnLoader):
    """In this scenario, the repository has been updated with new changes.
       The loading visit should result in new objects stored and 1 new
       snapshot.

    """
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')
        self.loader = SvnLoaderUpdateNoStorage()

    @istest
    def process_repository(self):
        """Process updated repository should yield new objects

        """
        # when
        self.loader.load(svn_url=self.svn_mirror_url,
                         destination_path=self.destination_path)

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

        self.assertRevisionsOk(expected_revisions)

        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITTest5(BaseTestSvnLoader):
    """Context:

       - Repository already injected with successfull data
       - New visit from scratch done with successfull load

    """
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')
        self.loader = SvnLoaderUpdateNoStorage()

    @istest
    def load(self):
        """Load an existing repository from scratch yields same swh objects

        """
        # when
        self.loader.load(svn_url=self.svn_mirror_url,
                         destination_path=self.destination_path,
                         start_from_scratch=True)

        # then
        # we got the previous run's last revision (rev 6)
        # but we do not inspect that as we start from from scratch so
        # we should have all revisions so 11
        self.assertEquals(len(self.loader.all_revisions), 11)
        self.assertEquals(len(self.loader.all_releases), 0)

        expected_revisions = {
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
        }

        self.assertRevisionsOk(expected_revisions)

        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderWithPreviousRevisionNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context: Load a known svn repository using the swh policy with its
    history altered so we do not update it.

    """
    def swh_latest_snapshot_revision(self, origin_id, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'snapshot': None,
            'revision': {
                'id': hashutil.hash_to_bytes(
                    '4876cb10aec6f708f7466dddf547567b65f6c39c'),
                'parents': [hashutil.hash_to_bytes(
                    'a3a577948fdbda9d1061913b77a1588695eadb41')],
                'directory': hashutil.hash_to_bytes(
                    '0deab3023ac59398ae467fc4bff5583008af1ee2'),
                'target_type': 'revision',
                'metadata': {
                    'extra_headers': [
                        ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],  # noqa
                        ['svn_revision', '6']
                    ]
                }
            }
        }


class SvnLoaderITTest6(BaseTestSvnLoader):
    """Context:
       - repository already visited with load successfull
       - Changes on existing repository
       - New Visit done with successful new data

    """
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')
        self.loader = SvnLoaderWithPreviousRevisionNoStorage()

    @istest
    def load(self):
        """Load from partial previous visit result in new changes

        """
        # when
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path)

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

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITest7(BaseTestSvnLoader):
    """Context:
       - repository already visited with load successfull
       - Changes on existing repository
       - New Visit done with successful new data

    """
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')
        self.loader = SvnLoaderUpdateNoStorage()

    @istest
    def load(self):
        """Load known and partial repository should start from last visit

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
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            swh_revision=previous_unfinished_revision)

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

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderUpdateLessRecentNoStorage(TestSvnLoader, SWHSvnLoader):
    """Context:
        Load a known svn repository.  The last visit seen is less
        recent than a previous unfinished crawl.

    """
    def swh_latest_snapshot_revision(self, origin_id, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'snapshot': None,
            'revision': {
                'id': hashutil.hash_to_bytes(
                    'a3a577948fdbda9d1061913b77a1588695eadb41'),
                'parents': [hashutil.hash_to_bytes(
                    '3f51abf3b3d466571be0855dfa67e094f9ceff1b')],
                'directory': hashutil.hash_to_bytes(
                    '7dc52cc04c3b8bd7c085900d60c159f7b846f866'),
                'target_type': 'revision',
                'metadata': {
                    'extra_headers': [
                        ['svn_repo_uuid',
                         '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                        ['svn_revision', '5']
                    ]
                }
            }
        }


class SvnLoaderITest8(BaseTestSvnLoader):
    """Context:

       - Previous visit on existing repository done
       - Starting the loading from the last unfinished visit
       - New objects are created (1 new snapshot)

    """
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')
        self.loader = SvnLoaderUpdateLessRecentNoStorage()

    @istest
    def load(self):
        """Load repository should yield revisions starting from last visit

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
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            swh_revision=previous_unfinished_revision)

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

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderTTest9(BaseTestSvnLoader):
    """Check that a svn repo containing a versioned file with CRLF line
       endings with svn:eol-style property set to 'native' (this is a
       violation of svn specification as the file should have been
       stored with LF line endings) can be loaded anyway.

    """
    def setUp(self):
        super().setUp(archive_name='mediawiki-repo-r407-eol-native-crlf.tgz',
                      filename='mediawiki-repo-r407-eol-native-crlf')
        self.loader = SvnLoaderNoStorage()

    @istest
    def process_repository(self):
        """Load repository with CRLF endings (svn:eol-style: native) is ok

        """ # noqa
        # when
        self.loader.load(svn_url=self.svn_mirror_url,
                         destination_path=self.destination_path)

        expected_revisions = {
            '7da4975c363101b819756d33459f30a866d01b1b': 'f63637223ee0f7d4951ffd2d4d9547a4882c5d8b' # noqa
        }
        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITest10(BaseTestSvnLoader): # noqa
    """Check that a svn repo containing a versioned file with mixed
    CRLF/LF line endings with svn:eol-style property set to 'native'
    (this is a violation of svn specification as mixed line endings
    for textual content should not be stored when the svn:eol-style
    property is set) can be loaded anyway.

    """
    def setUp(self):
        super().setUp(
            archive_name='pyang-repo-r343-eol-native-mixed-lf-crlf.tgz',
            filename='pyang-repo-r343-eol-native-mixed-lf-crlf')
        self.loader = SvnLoaderNoStorage()

    @istest
    def load(self):
        """Load repo with mixed CRLF/LF endings (svn:eol-style:native) is ok

        """
        self.loader.load(svn_url=self.svn_mirror_url,
                         destination_path=self.destination_path)

        expected_revisions = {
            '9c6962eeb9164a636c374be700672355e34a98a7': '16aa6b6271f3456d4643999d234cf39fe3d0cc5a' # noqa
        }

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITest11(BaseTestSvnLoader):
    """Context:

       - Repository with svn:external (which is not deal with for now)
       - Visit is partial with as much data loaded as possible

    """
    def setUp(self):
        super().setUp(archive_name='pkg-gourmet-with-external-id.tgz')
        self.loader = SvnLoaderNoStorage()

    @istest
    def load(self):
        """Repository with svn:externals property, will stop raising an error

        """
        previous_unfinished_revision = None

        # when
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            swh_revision=previous_unfinished_revision)

        # then repositories holds 21 revisions, but the last commit
        # one holds an 'svn:externals' property which will make the
        # loader-svn stops at the last revision prior to the bad one
        self.assertEquals(len(self.loader.all_revisions), 20)
        self.assertEquals(len(self.loader.all_releases), 0)

        last_revision = '82a7a4a09f9549223429143ba36ad77375e33c5c'
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
            'ffa901b69ca0f46a2261f42948838d19709cb9f8': 'c3c98df624733fef4e592bef983f93e2ed02b179',  # noqa
            '0148ae3eaa520b73a50802c59f3f416b7a36cf8c': '844d4646d6c2b4f3a3b2b22ab0ee38c7df07bab2',  # noqa
            last_revision: '0de6e75d2b79ec90d00a3a7611aa3861b2e4aa5e',  # noqa
        }

        # The last revision being the one used later to start back from
        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'partial')


class SvnLoaderITest12(BaseTestSvnLoader):
    """Edge cases:
       - first create a file and commit it.
         Remove it, then add folder holding the same name, commit.
       - do the same scenario with symbolic link (instead of file)

    """
    def setUp(self):
        super().setUp(
            archive_name='pkg-gourmet-with-edge-case-links-and-files.tgz')
        self.loader = SvnLoaderNoStorage()

    @istest
    def load(self):
        """File/Link removed prior to folder with same name creation is ok

        """
        previous_unfinished_revision = None

        # when
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            swh_revision=previous_unfinished_revision)

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

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class SvnLoaderITTest13(BaseTestSvnLoader):
    """Edge cases:
       - wrong symbolic link
       - wrong symbolic link with empty space names

    """
    def setUp(self):
        super().setUp(
            archive_name='pkg-gourmet-with-wrong-link-cases.tgz')
        self.loader = SvnLoaderNoStorage()

    @istest
    def load(self):
        """Wrong link or empty space-named link should be ok

        """
        # when
        self.loader.load(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path)

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

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')
