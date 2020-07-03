# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import os
import subprocess

from typing import Optional

from swh.loader.core.tests import BaseLoaderTest
from swh.loader.tests.common import assert_last_visit_matches
from swh.loader.package.tests.common import check_snapshot, get_stats

from swh.loader.svn.loader import (
    DEFAULT_BRANCH,
    SvnLoader,
    SvnLoaderFromRemoteDump,
    build_swh_snapshot,
)
from swh.model import hashutil
from swh.model.model import Origin, Snapshot


def test_build_swh_snapshot():
    rev_id = hashutil.hash_to_bytes("3f51abf3b3d466571be0855dfa67e094f9ceff1b")
    snap = build_swh_snapshot(rev_id)

    assert isinstance(snap, Snapshot)

    expected_snapshot = Snapshot.from_dict(
        {"branches": {DEFAULT_BRANCH: {"target": rev_id, "target_type": "revision",}}}
    )
    assert snap == expected_snapshot


_LOADER_TEST_CONFIG = {
    "check_revision": {"limit": 100, "status": False},
    "debug": False,
    "log_db": "dbname=softwareheritage-log",
    "save_data": False,
    "save_data_path": "",
    "temp_directory": "/tmp",
    "max_content_size": 100 * 1024 * 1024,
    "storage": {
        "cls": "pipeline",
        "steps": [
            {"cls": "retry",},
            {"cls": "filter",},
            {
                "cls": "buffer",
                "min_batch_size": {
                    "content": 10000,
                    "content_bytes": 1073741824,
                    "directory": 2500,
                    "revision": 10,
                    "release": 100,
                },
            },
            {"cls": "memory"},
        ],
    },
}

GOURMET_SNAPSHOT = hashutil.hash_to_bytes("889cacc2731e3312abfb2b1a0c18ade82a949e07")

GOURMET_FLAG_SNAPSHOT = hashutil.hash_to_bytes(
    "0011223344556677889900112233445566778899"
)

GOURMET_UPDATES_SNAPSHOT = hashutil.hash_to_bytes(
    "11086d15317014e43d2438b7ffc712c44f1b8afe"
)

GOURMET_EXTERNALS_SNAPSHOT = hashutil.hash_to_bytes(
    "19cb68d0a3f22372e2b7017ea5e2a2ea5ae3e09a"
)

GOURMET_EDGE_CASES_SNAPSHOT = hashutil.hash_to_bytes(
    "18e60982fe521a2546ab8c3c73a535d80462d9d0"
)

GOURMET_WRONG_LINKS_SNAPSHOT = hashutil.hash_to_bytes(
    "b17f38acabb90f066dedd30c29f01a02af88a5c4"
)

MEDIAWIKI_SNAPSHOT = hashutil.hash_to_bytes("d6d6e9703f157c5702d9a4a5dec878926ed4ab76")

PYANG_SNAPSHOT = hashutil.hash_to_bytes("6d9590de11b00a5801de0ff3297c5b44bbbf7d24")


class SvnLoaderTest(SvnLoader):
    """An SVNLoader with no persistence.

    Context:
        Load a new svn repository using the swh policy (so no update).

    """

    def __init__(
        self,
        url,
        last_snp_rev={},
        destination_path=None,
        start_from_scratch=False,
        swh_revision=None,
    ):
        super().__init__(
            url,
            destination_path=destination_path,
            start_from_scratch=start_from_scratch,
            swh_revision=swh_revision,
        )
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

    def setUp(
        self,
        archive_name="pkg-gourmet.tgz",
        filename="pkg-gourmet",
        loader=None,
        snapshot=None,
        type="default",
        start_from_scratch=False,
        swh_revision=None,
    ):
        super().setUp(
            archive_name=archive_name,
            filename=filename,
            prefix_tmp_folder_name="swh.loader.svn.",
            start_path=os.path.dirname(__file__),
        )
        self.svn_mirror_url = self.repo_url
        if type == "default":
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
                swh_revision=swh_revision,
            )
        self.storage = self.loader.storage


def prepare_repository_from_archive(
    archive_path: str, filename: Optional[str] = None, tmp_path: str = "/tmp"
) -> str:
    # uncompress folder/repositories/dump for the loader to ingest
    subprocess.check_output(["tar", "xf", archive_path, "-C", tmp_path])
    # build the origin url (or some derivative form)
    _fname = filename if filename else os.path.basename(archive_path)
    repo_url = f"file://{tmp_path}/{_fname}"
    return repo_url


def test_loader_svn_new_visit(swh_config, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn", snapshot=GOURMET_SNAPSHOT,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 19,
        "directory": 17,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 6,
        "skipped_content": 0,
        "snapshot": 1,
    }

    expected_snapshot = {
        "id": GOURMET_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "4876cb10aec6f708f7466dddf547567b65f6c39c",
                "target_type": "revision",
            }
        },
    }

    check_snapshot(expected_snapshot, loader.storage)


def test_loader_svn_2_visits_no_change(swh_config, datadir, tmp_path):
    """Visit multiple times a repository with no change should yield the same snapshot

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn", snapshot=GOURMET_SNAPSHOT,
    )

    # FIXME: This should be uneventful here as there is no change in between visits...
    assert loader.load() == {"status": "eventful"}
    visit_status2 = assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn", snapshot=GOURMET_SNAPSHOT,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot == visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats["origin_visit"] == 1 + 1  # computed twice the same snapshot
    assert stats["snapshot"] == 1

    # even starting from previous revision...
    revs = list(
        loader.storage.revision_get(
            [hashutil.hash_to_bytes("95edacc8848369d6fb1608e887d6d2474fd5224f")]
        )
    )
    start_revision = revs[0]
    assert start_revision is not None

    loader = SvnLoader(repo_url, swh_revision=start_revision)
    assert loader.load() == {"status": "eventful"}

    stats = get_stats(loader.storage)
    assert stats["origin_visit"] == 2 + 1
    # ... with no change in repository, this yields the same snapshot
    assert stats["snapshot"] == 1

    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn", snapshot=GOURMET_SNAPSHOT,
    )


_LAST_SNP_REV = {
    "snapshot": Snapshot.from_dict({"id": GOURMET_FLAG_SNAPSHOT, "branches": {}}),
    "revision": {
        "id": hashutil.hash_to_bytes("4876cb10aec6f708f7466dddf547567b65f6c39c"),
        "parents": (
            hashutil.hash_to_bytes("a3a577948fdbda9d1061913b77a1588695eadb41"),
        ),
        "directory": hashutil.hash_to_bytes("0deab3023ac59398ae467fc4bff5583008af1ee2"),
        "target_type": "revision",
        "metadata": {
            "extra_headers": [
                ["svn_repo_uuid", "3187e211-bb14-4c82-9596-0b59d67cd7f4"],
                ["svn_revision", "6"],
            ]
        },
    },
}


class SvnLoaderTest3(BaseSvnLoaderTest):
    """In this scenario, the dump has been tampered with to modify the
       commit log.  This results in a hash divergence which is
       detected at startup.

       In effect, that stops the loading and do nothing.

    """

    def setUp(self):
        last_snp_rev = copy.deepcopy(_LAST_SNP_REV)
        last_snp_rev["snapshot"] = None
        # Changed the revision id's hash to simulate history altered
        last_snp_rev["revision"]["id"] = hashutil.hash_to_bytes(
            "badbadbadbadf708f7466dddf547567b65f6c39d"
        )
        # the svn repository pkg-gourmet has been updated with changes
        super().setUp(
            archive_name="pkg-gourmet-with-updates.tgz", snapshot=last_snp_rev
        )

    def test_load(self):
        """Load known repository with history altered should do nothing

        """
        # when
        assert self.loader.load() == {"status": "failed"}

        # then
        # we got the previous run's last revision (rev 6)
        # so 2 news + 1 old
        self.assertCountContents(0)
        self.assertCountDirectories(0)
        self.assertCountRevisions(0)
        self.assertCountReleases(0)
        self.assertCountSnapshots(0)
        self.assertEqual(self.loader.visit_status(), "partial")

        visit_status = assert_last_visit_matches(
            self.storage, self.repo_url, status="partial", type="svn"
        )
        assert visit_status.snapshot is None


def test_loader_svn_visit_with_changes(swh_config, datadir, tmp_path):
    """In this scenario, the repository has been updated with new changes.
       The loading visit should result in new objects stored and 1 new
       snapshot.

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_initial_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path
    )

    # repo_initial_url becomes the origin_url we want to visit some more below
    loader = SvnLoader(repo_initial_url)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_initial_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT,
    )

    archive_path = os.path.join(datadir, "pkg-gourmet-with-updates.tgz")
    repo_updated_url = prepare_repository_from_archive(
        archive_path, "pkg-gourmet", tmp_path
    )

    loader = SvnLoader(repo_updated_url, origin_url=repo_initial_url,)

    assert loader.load() == {"status": "eventful"}
    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_updated_url,
        status="full",
        type="svn",
        snapshot=GOURMET_UPDATES_SNAPSHOT,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot != visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 22,
        "directory": 28,
        "origin": 1,
        "origin_visit": 2,
        "person": 2,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 2,
    }

    expected_snapshot = {
        "id": GOURMET_UPDATES_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "171dc35522bfd17dda4e90a542a0377fb2fc707a",
                "target_type": "revision",
            }
        },
    }

    check_snapshot(expected_snapshot, loader.storage)

    # Start from scratch loading yields the same result

    loader = SvnLoader(
        repo_updated_url, origin_url=repo_initial_url, start_from_scratch=True
    )
    assert loader.load() == {"status": "eventful"}
    visit_status3 = assert_last_visit_matches(
        loader.storage,
        repo_updated_url,
        status="full",
        type="svn",
        snapshot=GOURMET_UPDATES_SNAPSHOT,
    )
    assert visit_status2.date < visit_status3.date
    assert visit_status3.snapshot == visit_status2.snapshot
    check_snapshot(expected_snapshot, loader.storage)

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1  # always the same visit
    assert stats["origin_visit"] == 2 + 1  # 1 more visit
    assert stats["snapshot"] == 2  # no new snapshot


def test_loader_svn_visit_start_from_revision(swh_config, datadir, tmp_path):
    """Starting from existing revision, next visit on changed repo should yield 1 new
       snapshot.

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_initial_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path
    )

    # repo_initial_url becomes the origin_url we want to visit some more below
    loader = SvnLoader(repo_initial_url)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_initial_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT,
    )

    revs = list(
        loader.storage.revision_get(
            [hashutil.hash_to_bytes("95edacc8848369d6fb1608e887d6d2474fd5224f")]
        )
    )
    start_revision = revs[0]
    assert start_revision is not None

    archive_path = os.path.join(datadir, "pkg-gourmet-with-updates.tgz")
    repo_updated_url = prepare_repository_from_archive(
        archive_path, "pkg-gourmet", tmp_path
    )

    # we'll start from start_revision
    loader = SvnLoader(
        repo_updated_url, origin_url=repo_initial_url, swh_revision=start_revision
    )

    assert loader.load() == {"status": "eventful"}

    # nonetheless, we obtain the same snapshot (as previous tests on that repository)
    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_updated_url,
        status="full",
        type="svn",
        snapshot=GOURMET_UPDATES_SNAPSHOT,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot != visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 22,
        "directory": 28,
        "origin": 1,
        "origin_visit": 2,
        "person": 2,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 2,
    }

    expected_snapshot = {
        "id": GOURMET_UPDATES_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "171dc35522bfd17dda4e90a542a0377fb2fc707a",
                "target_type": "revision",
            }
        },
    }

    check_snapshot(expected_snapshot, loader.storage)


def test_loader_svn_visit_with_eol_style(swh_config, datadir, tmp_path):
    """Check that a svn repo containing a versioned file with CRLF line
       endings with svn:eol-style property set to 'native' (this is a
       violation of svn specification as the file should have been
       stored with LF line endings) can be loaded anyway.

    """
    archive_name = "mediawiki-repo-r407-eol-native-crlf"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    expected_snapshot = {
        "id": MEDIAWIKI_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "7da4975c363101b819756d33459f30a866d01b1b",
                "target_type": "revision",
            }
        },
    }
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=MEDIAWIKI_SNAPSHOT,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1


def test_loader_svn_visit_with_mixed_crlf_lf(swh_config, datadir, tmp_path):
    """Check that a svn repo containing a versioned file with mixed
    CRLF/LF line endings with svn:eol-style property set to 'native'
    (this is a violation of svn specification as mixed line endings
    for textual content should not be stored when the svn:eol-style
    property is set) can be loaded anyway.

    """
    archive_name = "pyang-repo-r343-eol-native-mixed-lf-crlf"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    expected_snapshot = {
        "id": PYANG_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "9c6962eeb9164a636c374be700672355e34a98a7",
                "target_type": "revision",
            }
        },
    }
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn", snapshot=PYANG_SNAPSHOT,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1


def test_loader_svn_with_external_properties(swh_config, datadir, tmp_path):
    """Repository with svn:external properties cannot be fully ingested yet

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, "pkg-gourmet-with-external-id.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    # repositoy holds 21 revisions, but the last commit holds an 'svn:externals'
    # property which will make the loader-svn stops at the last revision prior to the
    # bad one
    expected_snapshot = {
        "id": GOURMET_EXTERNALS_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "82a7a4a09f9549223429143ba36ad77375e33c5c",
                "target_type": "revision",
            }
        },
    }
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="partial",
        type="svn",
        snapshot=GOURMET_EXTERNALS_SNAPSHOT,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 21 - 1  # commit with the svn:external property


def test_loader_svn_with_symlink(swh_config, datadir, tmp_path):
    """Repository with symlinks should be ingested ok

    Edge case:
       - first create a file and commit it.
         Remove it, then add folder holding the same name, commit.
       - do the same scenario with symbolic link (instead of file)

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(
        datadir, "pkg-gourmet-with-edge-case-links-and-files.tgz"
    )
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    expected_snapshot = {
        "id": GOURMET_EDGE_CASES_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "3f43af2578fccf18b0d4198e48563da7929dc608",
                "target_type": "revision",
            }
        },
    }
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_EDGE_CASES_SNAPSHOT,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 19


def test_loader_svn_with_wrong_symlinks(swh_config, datadir, tmp_path):
    """Repository with wrong symlinks should be ingested ok nonetheless

    Edge case:
       - wrong symbolic link
       - wrong symbolic link with empty space names

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, "pkg-gourmet-with-wrong-link-cases.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    expected_snapshot = {
        "id": GOURMET_WRONG_LINKS_SNAPSHOT,
        "branches": {
            "HEAD": {
                "target": "cf30d3bb9d5967d0a2bbeacc405f10a5dd9b138a",
                "target_type": "revision",
            }
        },
    }
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_WRONG_LINKS_SNAPSHOT,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 21


class SvnLoaderTestFromRemoteDump(SvnLoaderTest, SvnLoaderFromRemoteDump):
    pass


class SvnLoaderFromRemoteDumpTest(BaseSvnLoaderTest):
    """
    Check that the results obtained with the remote svn dump loader
    and the base svn loader are the same.
    """

    def setUp(self):
        _LOADER_TEST_CONFIG["debug"] = True  # to avoid cleanup in between load
        super().setUp(archive_name="pkg-gourmet.tgz", type="remote")

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

        assert_last_visit_matches(
            self.storage,
            self.repo_url,
            status="full",
            type="svn",
            snapshot=GOURMET_SNAPSHOT,
        )

        assert_last_visit_matches(
            base_loader.storage,
            self.repo_url,
            status="full",
            type="svn",
            snapshot=GOURMET_SNAPSHOT,
        )


class SvnLoaderTest14(BaseSvnLoaderTest):
    """Edge cases: The repository held some user defined svn-properties
       with special encodings, this prevented the repository from
       being loaded even though we do not ingest those information.

    """

    def setUp(self):
        super().setUp(archive_name="httthttt.tgz", filename="httthttt")

    def test_load(self):
        """Decoding user defined svn properties error should not fail loading

        """
        # when
        assert self.loader.load() == {"status": "eventful"}

        self.assertCountRevisions(7, "7 svn commits")
        self.assertCountReleases(0)

        last_revision = "604a17dbb15e8d7ecb3e9f3768d09bf493667a93"

        expected_revisions = {
            "e6ae8487c6d14df9e6cb7196c6aac045798fd5be": "75ed58f260bfa4102d0e09657803511f5f0ab372",  # noqa
            "e1e3314e0e9c9d17e6a3f60d6662f48f0e3c2fa3": "7bfb95cef68c1affe8d7f786353213d92abbb2b7",  # noqa
            "1632fd38a8653e9b607c00feb93a41faddfb544c": "cd6de65c84d9405e7ca45fead02aa10162e30727",  # noqa
            "0ad1ebbb92d00721644b0a46d6322d18dbcba848": "cd6de65c84d9405e7ca45fead02aa10162e30727",  # noqa
            "94b87c97697d178a9311b018daa5179f7d4ba31e": "c2128108adecb59a0144339c2e701cd8118cff5a",  # noqa
            "bd741cf22f0642d88cd0d8b545e8896b898c692d": "c2128108adecb59a0144339c2e701cd8118cff5a",  # noqa
            last_revision: "f051d60256b2d89a0ca2704d6f91ad1b0ab44e02",
        }

        self.assertRevisionsContain(expected_revisions)

        expected_snapshot_id = "70487267f682c07e52a2371061369b6cf5bffa47"
        expected_branches = {
            "HEAD": {"target": last_revision, "target_type": "revision"}
        }

        self.assertSnapshotEqual(expected_snapshot_id, expected_branches)

        self.assertEqual(self.loader.visit_status(), "full")

        assert_last_visit_matches(
            self.storage,
            self.repo_url,
            status="full",
            type="svn",
            snapshot=hashutil.hash_to_bytes(expected_snapshot_id),
        )
