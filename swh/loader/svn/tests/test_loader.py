# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile
import unittest

from nose.tools import istest

from swh.core import hashutil
from swh.loader.svn.loader import GitSvnSvnLoader, SWHSvnLoader


# Define loaders with no storage
# They'll just accumulate the data in place
# Only for testing purposes.

class TestSvnLoader:
    """Mixin class to inhibit the persistence and keep in memory the data
    sent for storage.

    cf. GitSvnLoaderNoStorage, SWHSvnLoaderNoStorage

    """
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin)
        # We don't want to persist any result in this test context
        self.config['send_contents'] = False
        self.config['send_directories'] = False
        self.config['send_revisions'] = False
        self.config['send_releases'] = False
        self.config['send_occurrences'] = False
        # Init the state
        self.all_contents = []
        self.all_directories = []
        self.all_revisions = []
        self.all_releases = []
        self.all_occurrences = []

    def maybe_load_contents(self, all_contents):
        self.all_contents.extend(all_contents)

    def maybe_load_directories(self, all_directories):
        self.all_directories.extend(all_directories)

    def maybe_load_revisions(self, all_revisions):
        self.all_revisions.extend(all_revisions)

    def maybe_load_releases(self, releases):
        raise ValueError('If called, the test must break.')

    def maybe_load_occurrences(self, all_occurrences):
        self.all_occurrences.extend(all_occurrences)


class GitSvnLoaderNoStorage(TestSvnLoader, GitSvnSvnLoader):
    """A GitSvnLoader with no persistence.

    Context:
        Load an svn repository using the git-svn policy.

    """
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin)


class SWHSvnLoaderNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context:
        Load a new svn repository using the swh policy (so no update).

    """
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin)

    def swh_previous_revision(self):
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
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin)

    def swh_previous_revision(self):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            'id': hashutil.hex_to_hash(
                '4876cb10aec6f708f7466dddf547567b65f6c39c'),
            'parents': [hashutil.hex_to_hash(
                'a3a577948fdbda9d1061913b77a1588695eadb41')],
            'directory': hashutil.hex_to_hash(
                '0deab3023ac59398ae467fc4bff5583008af1ee2'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', b'6']
                ]
            }
        }


class SWHSvnLoaderUpdateHistoryAlteredNoStorage(TestSvnLoader, SWHSvnLoader):
    """An SWHSVNLoader with no persistence.

    Context: Load a known svn repository using the swh policy with its
    history altered so we do not update it.

    """
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin)

    def swh_previous_revision(self):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SWHSvnLoaderITTest

        """
        return {
            # Changed the revision id's hash to simulate history altered
            'id': hashutil.hex_to_hash(
                'badbadbadbadf708f7466dddf547567b65f6c39d'),
            'parents': [hashutil.hex_to_hash(
                'a3a577948fdbda9d1061913b77a1588695eadb41')],
            'directory': hashutil.hex_to_hash(
                '0deab3023ac59398ae467fc4bff5583008af1ee2'),
            'target_type': 'revision',
            'metadata': {
                'extra_headers': [
                    ['svn_repo_uuid', '3187e211-bb14-4c82-9596-0b59d67cd7f4'],
                    ['svn_revision', b'6']
                ]
            }
        }


class BaseTestLoader(unittest.TestCase):
    """Base test loader class.

    In its setup, it's uncompressing a local svn mirror to /tmp.

    """
    def setUp(self, archive_name='pkg-gourmet.tgz', filename='pkg-gourmet'):
        self.tmp_root_path = tempfile.mkdtemp()

        start_path = os.path.dirname(__file__)
        svn_mirror_repo = os.path.join(start_path,
                                       '../../../../..',
                                       'swh-storage-testdata',
                                       'svn-folders',
                                       archive_name)

        # uncompress the sample folder
        subprocess.check_output(
            ['tar', 'xvf', svn_mirror_repo, '-C', self.tmp_root_path],
        )

        self.svn_mirror_url = 'file://' + self.tmp_root_path + '/' + filename
        self.destination_path = os.path.join(
            self.tmp_root_path, 'working-copy')

    def tearDown(self):
        shutil.rmtree(self.tmp_root_path)


class GitSvnLoaderITTest(BaseTestLoader):
    def setUp(self):
        super().setUp()

        self.origin = {'id': 1, 'type': 'svn', 'url': 'file:///dev/null'}

        self.loader = GitSvnLoaderNoStorage(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            origin=self.origin)

    @istest
    def process_repository(self):
        """Process a repository with gitsvn policy should be ok."""
        # when
        self.loader.process_repository()

        # then
        self.assertEquals(len(self.loader.all_revisions), 6)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_occurrences), 1)

        last_revision = 'bad4a83737f337d47e0ba681478214b07a707218'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash  # noqa
            '22c0fa5195a53f2e733ec75a9b6e9d1624a8b771': '4b825dc642cb6eb9a060e54bf8d69288fbee4904',  # noqa
            '17a631d474f49bbebfdf3d885dcde470d7faafd7': '4b825dc642cb6eb9a060e54bf8d69288fbee4904',  # noqa
            'c8a9172b2a615d461154f61158180de53edc6070': '4b825dc642cb6eb9a060e54bf8d69288fbee4904',  # noqa
            '7c8f83394b6e8966eb46f0d3416c717612198a4b': '4b825dc642cb6eb9a060e54bf8d69288fbee4904',  # noqa
            '852547b3b2bb76c8582cee963e8aa180d552a15c': 'ab047e38d1532f61ff5c3621202afc3e763e9945',  # noqa
            last_revision:                              '9bcfc25001b71c333b4b5a89224217de81c56e2e',  # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)

        occ = self.loader.all_occurrences[0]
        self.assertEquals(hashutil.hash_to_hex(occ['target']),
                          last_revision)
        self.assertEquals(occ['origin'], self.origin['id'])


class SWHSvnLoaderNewRepositoryITTest(BaseTestLoader):
    def setUp(self):
        super().setUp()

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.loader = SWHSvnLoaderNoStorage(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            origin=self.origin)

    @istest
    def process_repository(self):
        """Process a new repository with swh policy should be ok.

        """
        # when
        self.loader.process_repository()

        # then
        self.assertEquals(len(self.loader.all_revisions), 6)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_occurrences), 1)

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

        occ = self.loader.all_occurrences[0]
        self.assertEquals(hashutil.hash_to_hex(occ['target']), last_revision)
        self.assertEquals(occ['origin'], self.origin['id'])


class SWHSvnLoaderUpdateWithNoChangeITTest(BaseTestLoader):
    def setUp(self):
        super().setUp()

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.loader = SWHSvnLoaderUpdateNoStorage(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            origin=self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy and no new data should
        be ok.

        """
        # when
        self.loader.process_repository()

        # then
        self.assertEquals(len(self.loader.all_revisions), 0)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_occurrences), 0)


class SWHSvnLoaderUpdateWithHistoryAlteredITTest(BaseTestLoader):
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.loader = SWHSvnLoaderUpdateHistoryAlteredNoStorage(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            origin=self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy and history altered should
        stop and do nothing.

        """
        # when
        self.loader.process_repository()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 news + 1 old
        self.assertEquals(len(self.loader.all_revisions), 0)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_occurrences), 0)


class SWHSvnLoaderUpdateWithChangesITTest(BaseTestLoader):
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz')

        self.origin = {'id': 2, 'type': 'svn', 'url': 'file:///dev/null'}

        self.loader = SWHSvnLoaderUpdateNoStorage(
            svn_url=self.svn_mirror_url,
            destination_path=self.destination_path,
            origin=self.origin)

    @istest
    def process_repository(self):
        """Process a known repository with swh policy and new data should
        yield new revisions and occurrence.

        """
        # when
        self.loader.process_repository()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 news + 1 old
        self.assertEquals(len(self.loader.all_revisions), 2)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_occurrences), 1)

        last_revision = '38d81702cb28db4f1a6821e64321e5825d1f7fd6'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '7f5bc909c29d4e93d8ccfdda516e51ed44930ee1': '752c52134dcbf2fff13c7be1ce4e9e5dbf428a59',  # noqa
            last_revision:                              '39c813fb4717a4864bacefbd90b51a3241ae4140',  # noqa
        }

        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)

        occ = self.loader.all_occurrences[0]
        self.assertEquals(hashutil.hash_to_hex(occ['target']), last_revision)
        self.assertEquals(occ['origin'], self.origin['id'])
