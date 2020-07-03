# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess

from typing import Optional

from swh.loader.tests.common import assert_last_visit_matches
from swh.loader.package.tests.common import check_snapshot, get_stats

from swh.loader.svn.loader import (
    DEFAULT_BRANCH,
    SvnLoader,
    SvnLoaderFromRemoteDump,
    build_swh_snapshot,
)
from swh.model import hashutil
from swh.model.model import Snapshot


def test_build_swh_snapshot():
    rev_id = hashutil.hash_to_bytes("3f51abf3b3d466571be0855dfa67e094f9ceff1b")
    snap = build_swh_snapshot(rev_id)

    assert isinstance(snap, Snapshot)

    expected_snapshot = Snapshot.from_dict(
        {"branches": {DEFAULT_BRANCH: {"target": rev_id, "target_type": "revision",}}}
    )
    assert snap == expected_snapshot


GOURMET_SNAPSHOT = hashutil.hash_to_bytes("889cacc2731e3312abfb2b1a0c18ade82a949e07")

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

    assert loader.load() == {"status": "uneventful"}
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


def test_loader_tampered_repository(swh_config, datadir, tmp_path):
    """In this scenario, the dump has been tampered with to modify the
       commit log [1].  This results in a hash divergence which is
       detected at startup after a new run for the same origin.

       In effect, that stops the loading and do nothing.

    [1] Tampering with revision 6 log message following:

    ```
        tar xvf pkg-gourmet.tgz  # initial repository ingested
        cd pkg-gourmet/
        echo "Tampering with commit log message for fun and profit" > log.txt
        svnadmin setlog . -r 6 log.txt --bypass-hooks
        tar cvf pkg-gourmet-tampered-rev6-log.tgz pkg-gourmet/
    ```
    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)
    assert loader.load() == {"status": "eventful"}
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

    archive_path2 = os.path.join(datadir, "pkg-gourmet-tampered-rev6-log.tgz")
    repo_tampered_url = prepare_repository_from_archive(
        archive_path2, archive_name, tmp_path
    )

    loader2 = SvnLoader(repo_tampered_url, origin_url=repo_url)
    assert loader2.load() == {"status": "failed"}

    assert_last_visit_matches(
        loader2.storage, repo_url, status="partial", type="svn", snapshot=None,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 2
    assert stats["snapshot"] == 1


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


def test_loader_svn_loader_from_dump_archive(swh_config, datadir, tmp_path):
    """Repository with wrong symlinks should be ingested ok nonetheless

    Edge case:
       - wrong symbolic link
       - wrong symbolic link with empty space names

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loaderFromDump = SvnLoaderFromRemoteDump(repo_url)
    assert loaderFromDump.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loaderFromDump.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT,
    )

    origin_url = repo_url + "2"  # rename to another origin
    loader = SvnLoader(repo_url, origin_url=origin_url)
    assert loader.load() == {"status": "eventful"}  # because are working on new origin
    assert_last_visit_matches(
        loader.storage, origin_url, status="full", type="svn", snapshot=GOURMET_SNAPSHOT
    )

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

    stats = get_stats(loader.storage)
    assert stats["origin"] == 2  # created one more origin
    assert stats["origin_visit"] == 2
    assert stats["snapshot"] == 1

    loader = SvnLoader(repo_url)  # no change on the origin-url
    assert loader.load() == {"status": "uneventful"}
    assert_last_visit_matches(
        loader.storage, origin_url, status="full", type="svn", snapshot=GOURMET_SNAPSHOT
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 2
    assert stats["origin_visit"] == 3
    assert stats["snapshot"] == 1


def test_loader_user_defined_svn_properties(swh_config, datadir, tmp_path):
    """Edge cases: The repository held some user defined svn-properties with special
       encodings, this prevented the repository from being loaded even though we do not
       ingest those information.

    """
    archive_name = "httthttt"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(repo_url)

    assert loader.load() == {"status": "eventful"}
    expected_snapshot_id = hashutil.hash_to_bytes(
        "70487267f682c07e52a2371061369b6cf5bffa47"
    )
    expected_snapshot = {
        "id": expected_snapshot_id,
        "branches": {
            "HEAD": {
                "target": "604a17dbb15e8d7ecb3e9f3768d09bf493667a93",
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
        snapshot=expected_snapshot_id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 7
