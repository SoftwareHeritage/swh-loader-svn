# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import os

from swh.loader.core.tests import BaseLoaderTest
from swh.loader.svn.loader import (DEFAULT_BRANCH, SvnLoader,
                                   SvnLoaderFromRemoteDump, build_swh_snapshot)
from swh.model import hashutil
from swh.model.model import (
    Origin, Snapshot
)


def test_build_swh_snapshot():
    rev_id = hashutil.hash_to_bytes(
        '3f51abf3b3d466571be0855dfa67e094f9ceff1b')
    snap = build_swh_snapshot(rev_id)

    assert isinstance(snap, Snapshot)

    expected_snapshot = Snapshot.from_dict({
        'branches': {
            DEFAULT_BRANCH: {
                'target': rev_id,
                'target_type': 'revision',
            }
        }
    })
    assert snap == expected_snapshot


_LOADER_TEST_CONFIG = {
    'check_revision': {'limit': 100, 'status': False},
    'debug': False,
    'log_db': 'dbname=softwareheritage-log',
    'save_data': False,
    'save_data_path': '',
    'temp_directory': '/tmp',
    'max_content_size': 100 * 1024 * 1024,
    'storage': {
        'cls': 'pipeline',
        'steps': [
            {
                'cls': 'retry',
            },
            {
                'cls': 'filter',
            },
            {
                'cls': 'buffer',
                'min_batch_size': {
                    'content': 10000,
                    'content_bytes': 1073741824,
                    'directory': 2500,
                    'revision': 10,
                    'release': 100,
                },
            },
            {
                'cls': 'memory'
            },
        ]
    },
}

GOURMET_SNAPSHOT = hashutil.hash_to_bytes(
    '889cacc2731e3312abfb2b1a0c18ade82a949e07'
)

GOURMET_FLAG_SNAPSHOT = hashutil.hash_to_bytes(
    '0011223344556677889900112233445566778899'
)

GOURMET_UPDATES_SNAPSHOT = hashutil.hash_to_bytes(
    '11086d15317014e43d2438b7ffc712c44f1b8afe'
)

GOURMET_EXTERNALS_SNAPSHOT = hashutil.hash_to_bytes(
    '19cb68d0a3f22372e2b7017ea5e2a2ea5ae3e09a'
)

GOURMET_EDGE_CASES_SNAPSHOT = hashutil.hash_to_bytes(
    '18e60982fe521a2546ab8c3c73a535d80462d9d0'
)

GOURMET_WRONG_LINKS_SNAPSHOT = hashutil.hash_to_bytes(
    'b17f38acabb90f066dedd30c29f01a02af88a5c4'
)

MEDIAWIKI_SNAPSHOT = hashutil.hash_to_bytes(
    'd6d6e9703f157c5702d9a4a5dec878926ed4ab76'
)

PYANG_SNAPSHOT = hashutil.hash_to_bytes(
    '6d9590de11b00a5801de0ff3297c5b44bbbf7d24'
)


class SvnLoaderTest(SvnLoader):
    """An SVNLoader with no persistence.

    Context:
        Load a new svn repository using the swh policy (so no update).

    """
    def __init__(self, url, last_snp_rev={}, destination_path=None,
                 start_from_scratch=False, swh_revision=None):
        super().__init__(url, destination_path=destination_path,
                         start_from_scratch=start_from_scratch,
                         swh_revision=swh_revision)
        self.origin = Origin(url=url)
        self.last_snp_rev = last_snp_rev

    def parse_config_file(self, *args, **kwargs):
        return _LOADER_TEST_CONFIG

    def swh_latest_snapshot_revision(self, origin_url, prev_swh_revision=None):
        """Avoid the storage persistence call and return the expected previous
        revision for that repository.

        Check the following for explanation about the hashes:
        - test_loader.org for (swh policy).
        - cf. SvnLoaderTest

        """
        return self.last_snp_rev


class BaseSvnLoaderTest(BaseLoaderTest):
    """Base test loader class.

    In its setup, it's uncompressing a local svn mirror to /tmp.

    """
    def setUp(self, archive_name='pkg-gourmet.tgz', filename='pkg-gourmet',
              loader=None, snapshot=None, type='default',
              start_from_scratch=False, swh_revision=None):
        super().setUp(archive_name=archive_name, filename=filename,
                      prefix_tmp_folder_name='swh.loader.svn.',
                      start_path=os.path.dirname(__file__))
        self.svn_mirror_url = self.repo_url
        if type == 'default':
            loader_test_class = SvnLoaderTest
        else:
            loader_test_class = SvnLoaderTestFromRemoteDump

        if loader:
            self.loader = loader
        elif snapshot:
            self.loader = loader_test_class(
                self.svn_mirror_url,
                destination_path=self.destination_path,
                start_from_scratch=start_from_scratch,
                swh_revision=swh_revision,
                last_snp_rev=snapshot,
            )
        else:
            self.loader = loader_test_class(
                self.svn_mirror_url,
                destination_path=self.destination_path,
                start_from_scratch=start_from_scratch,
                swh_revision=swh_revision
            )
        self.storage = self.loader.storage


class SvnLoaderTest1(BaseSvnLoaderTest):
    """Load an unknown svn repository results in new data.

    """
    def test_load(self):
        """Load a new repository results in new swh object and snapshot

        """
        # when
        self.loader.load()

        # then
        self.assertCountRevisions(6)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


_LAST_SNP_REV = {
    'snapshot': Snapshot.from_dict({
        'id': GOURMET_FLAG_SNAPSHOT,
        'branches': {}
    }),
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


class SvnLoaderTest2(BaseSvnLoaderTest):
    """Load a visited repository with no new change results in no data
       change.

    """
    def setUp(self):
        super().setUp(snapshot=_LAST_SNP_REV)

    def test_load(self):
        """Load a repository without new changes results in same snapshot

        """
        # when
        self.loader.load()

        # then

        self.assertCountContents(0)
        self.assertCountDirectories(0)
        self.assertCountRevisions(0)
        self.assertCountReleases(0)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'uneventful'})
        self.assertEqual(self.loader.visit_status(), 'full')
        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_FLAG_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest3(BaseSvnLoaderTest):
    """In this scenario, the dump has been tampered with to modify the
       commit log.  This results in a hash divergence which is
       detected at startup.

       In effect, that stops the loading and do nothing.

    """
    def setUp(self):
        last_snp_rev = copy.deepcopy(_LAST_SNP_REV)
        last_snp_rev['snapshot'] = None
        # Changed the revision id's hash to simulate history altered
        last_snp_rev['revision']['id'] = \
            hashutil.hash_to_bytes('badbadbadbadf708f7466dddf547567b65f6c39d')
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz',
                      snapshot=last_snp_rev)

    def test_load(self):
        """Load known repository with history altered should do nothing

        """
        # when
        self.loader.load()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 news + 1 old
        self.assertCountContents(0)
        self.assertCountDirectories(0)
        self.assertCountRevisions(0)
        self.assertCountReleases(0)
        self.assertCountSnapshots(0)
        self.assertEqual(self.loader.load_status(), {'status': 'uneventful'})
        self.assertEqual(self.loader.visit_status(), 'partial')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], None)
        self.assertEqual(visit['status'], 'partial')


class SvnLoaderTest4(BaseSvnLoaderTest):
    """In this scenario, the repository has been updated with new changes.
       The loading visit should result in new objects stored and 1 new
       snapshot.

    """
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz',
                      snapshot=_LAST_SNP_REV)

    def test_process_repository(self):
        """Process updated repository should yield new objects

        """
        # when
        self.loader.load()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertCountRevisions(5)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)

        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_UPDATES_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest5(BaseSvnLoaderTest):
    """Context:

       - Repository already injected with successful data
       - New visit from scratch done with successful load

    """
    def setUp(self):
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz',
                      snapshot=_LAST_SNP_REV,
                      start_from_scratch=True)

    def test_load(self):
        """Load an existing repository from scratch yields same swh objects

        """
        # when
        self.loader.load()

        # then
        # we got the previous run's last revision (rev 6)
        # but we do not inspect that as we start from from scratch so
        # we should have all revisions so 11
        self.assertCountRevisions(11)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)

        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_UPDATES_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest6(BaseSvnLoaderTest):
    """Context:
       - repository already visited with load successful
       - Changes on existing repository
       - New Visit done with successful new data

    """
    def setUp(self):
        last_snp_rev = {
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
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz',
                      snapshot=last_snp_rev)

    def test_load(self):
        """Load from partial previous visit result in new changes

        """
        # when
        self.loader.load()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertCountRevisions(5)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_UPDATES_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest7(BaseSvnLoaderTest):
    """Context:
       - repository already visited with load successful
       - Changes on existing repository
       - New Visit done with successful new data

    """
    def setUp(self):
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
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz',
                      snapshot=_LAST_SNP_REV,
                      swh_revision=previous_unfinished_revision)

    def test_load(self):
        """Load known and partial repository should start from last visit

        """

        # when
        self.loader.load()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertCountRevisions(5)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_UPDATES_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest8(BaseSvnLoaderTest):
    """Context:

       - Previous visit on existing repository done
       - Starting the loading from the last unfinished visit
       - New objects are created (1 new snapshot)

    """
    def setUp(self):
        last_snp_rev = {
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
        super().setUp(archive_name='pkg-gourmet-with-updates.tgz',
                      snapshot=last_snp_rev,
                      swh_revision=previous_unfinished_revision)

    def test_load(self):
        """Load repository should yield revisions starting from last visit

        """
        # when
        self.loader.load()

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 new
        self.assertCountRevisions(5)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_UPDATES_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest9(BaseSvnLoaderTest):
    """Check that a svn repo containing a versioned file with CRLF line
       endings with svn:eol-style property set to 'native' (this is a
       violation of svn specification as the file should have been
       stored with LF line endings) can be loaded anyway.

    """
    def setUp(self):
        super().setUp(archive_name='mediawiki-repo-r407-eol-native-crlf.tgz',
                      filename='mediawiki-repo-r407-eol-native-crlf')

    def test_process_repository(self):
        """Load repository with CRLF endings (svn:eol-style: native) is ok

        """
        # when
        self.loader.load()

        expected_revisions = {
            '7da4975c363101b819756d33459f30a866d01b1b': 'f63637223ee0f7d4951ffd2d4d9547a4882c5d8b' # noqa
        }
        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], MEDIAWIKI_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest10(BaseSvnLoaderTest): # noqa
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

    def test_load(self):
        """Load repo with mixed CRLF/LF endings (svn:eol-style:native) is ok

        """
        self.loader.load()

        expected_revisions = {
            '9c6962eeb9164a636c374be700672355e34a98a7': '16aa6b6271f3456d4643999d234cf39fe3d0cc5a' # noqa
        }

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], PYANG_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest11(BaseSvnLoaderTest):
    """Context:

       - Repository with svn:external (which is not deal with for now)
       - Visit is partial with as much data loaded as possible

    """
    def setUp(self):
        previous_unfinished_revision = None
        super().setUp(archive_name='pkg-gourmet-with-external-id.tgz',
                      swh_revision=previous_unfinished_revision)

    def test_load(self):
        """Repository with svn:externals property, will stop raising an error

        """

        # when
        self.loader.load()

        # then repositories holds 21 revisions, but the last commit
        # one holds an 'svn:externals' property which will make the
        # loader-svn stops at the last revision prior to the bad one
        self.assertCountRevisions(20)
        self.assertCountReleases(0)

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
        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'partial')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_EXTERNALS_SNAPSHOT)
        self.assertEqual(visit['status'], 'partial')


class SvnLoaderTest12(BaseSvnLoaderTest):
    """Edge cases:
       - first create a file and commit it.
         Remove it, then add folder holding the same name, commit.
       - do the same scenario with symbolic link (instead of file)

    """
    def setUp(self):
        previous_unfinished_revision = None
        super().setUp(
            archive_name='pkg-gourmet-with-edge-case-links-and-files.tgz',
            swh_revision=previous_unfinished_revision)

    def test_load(self):
        """File/Link removed prior to folder with same name creation is ok

        """

        # when
        self.loader.load()

        # then repositories holds 14 revisions, but the last commit
        self.assertCountRevisions(19)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_EDGE_CASES_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTest13(BaseSvnLoaderTest):
    """Edge cases:
       - wrong symbolic link
       - wrong symbolic link with empty space names

    """
    def setUp(self):
        super().setUp(
            archive_name='pkg-gourmet-with-wrong-link-cases.tgz')

    def test_load(self):
        """Wrong link or empty space-named link should be ok

        """
        # when
        self.loader.load()

        # then repositories holds 14 revisions, but the last commit
        self.assertCountRevisions(21)
        self.assertCountReleases(0)

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

        self.assertRevisionsContain(expected_revisions)
        self.assertCountSnapshots(1)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_WRONG_LINKS_SNAPSHOT)
        self.assertEqual(visit['status'], 'full')


class SvnLoaderTestFromRemoteDump(SvnLoaderTest, SvnLoaderFromRemoteDump):
    pass


class SvnLoaderFromRemoteDumpTest(BaseSvnLoaderTest):
    """
    Check that the results obtained with the remote svn dump loader
    and the base svn loader are the same.
    """
    def setUp(self):
        _LOADER_TEST_CONFIG['debug'] = True  # to avoid cleanup in between load
        super().setUp(archive_name='pkg-gourmet.tgz', type='remote')

    def test_load(self):
        """
        Compare results of remote dump loader and base loader
        """
        dump_loader = self.loader
        dump_loader.load()

        self.assertCountContents(19)
        self.assertCountDirectories(17)
        self.assertCountRevisions(6)
        self.assertCountSnapshots(1)

        base_loader = SvnLoaderTest(self.svn_mirror_url)
        base_loader.load()

        dump_storage_stat = dump_loader.storage.stat_counters()
        base_storage_stat = base_loader.storage.stat_counters()
        self.assertEqual(dump_storage_stat, base_storage_stat)

        visit = dump_loader.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_SNAPSHOT)

        visit = base_loader.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'], GOURMET_SNAPSHOT)


class SvnLoaderTest14(BaseSvnLoaderTest):
    """Edge cases: The repository held some user defined svn-properties
       with special encodings, this prevented the repository from
       being loaded even though we do not ingest those information.

    """
    def setUp(self):
        super().setUp(archive_name='httthttt.tgz', filename='httthttt')

    def test_load(self):
        """Decoding user defined svn properties error should not fail loading

        """
        # when
        self.loader.load()

        self.assertCountRevisions(7, '7 svn commits')
        self.assertCountReleases(0)

        last_revision = '604a17dbb15e8d7ecb3e9f3768d09bf493667a93'

        expected_revisions = {
            'e6ae8487c6d14df9e6cb7196c6aac045798fd5be': '75ed58f260bfa4102d0e09657803511f5f0ab372',  # noqa
            'e1e3314e0e9c9d17e6a3f60d6662f48f0e3c2fa3': '7bfb95cef68c1affe8d7f786353213d92abbb2b7',  # noqa
            '1632fd38a8653e9b607c00feb93a41faddfb544c': 'cd6de65c84d9405e7ca45fead02aa10162e30727',  # noqa
            '0ad1ebbb92d00721644b0a46d6322d18dbcba848': 'cd6de65c84d9405e7ca45fead02aa10162e30727',  # noqa
            '94b87c97697d178a9311b018daa5179f7d4ba31e': 'c2128108adecb59a0144339c2e701cd8118cff5a',  # noqa
            'bd741cf22f0642d88cd0d8b545e8896b898c692d': 'c2128108adecb59a0144339c2e701cd8118cff5a',  # noqa
            last_revision: 'f051d60256b2d89a0ca2704d6f91ad1b0ab44e02',
        }

        self.assertRevisionsContain(expected_revisions)

        expected_snapshot_id = '70487267f682c07e52a2371061369b6cf5bffa47'
        expected_branches = {
            'HEAD': {
                'target': last_revision,
                'target_type': 'revision'
            }
        }

        self.assertSnapshotEqual(expected_snapshot_id, expected_branches)

        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        visit = self.storage.origin_visit_get_latest(self.repo_url)
        self.assertEqual(visit['snapshot'],
                         hashutil.hash_to_bytes(expected_snapshot_id))
        self.assertEqual(visit['status'], 'full')
