# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum
from io import BytesIO
import os
import subprocess
from typing import Any, Dict, List

import pytest
from subvertpy import SubversionException, delta, repos
from subvertpy.ra import Auth, RemoteAccess, get_username_provider
from typing_extensions import TypedDict

from swh.loader.svn.loader import (
    SvnLoader,
    SvnLoaderFromDumpArchive,
    SvnLoaderFromRemoteDump,
)
from swh.loader.svn.svn import SvnRepo
from swh.loader.svn.utils import init_svn_repo_from_dump
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.from_disk import DentryPerms
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Snapshot, SnapshotBranch, TargetType

GOURMET_SNAPSHOT = Snapshot(
    id=hash_to_bytes("889cacc2731e3312abfb2b1a0c18ade82a949e07"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("4876cb10aec6f708f7466dddf547567b65f6c39c"),
            target_type=TargetType.REVISION,
        )
    },
)

GOURMET_UPDATES_SNAPSHOT = Snapshot(
    id=hash_to_bytes("11086d15317014e43d2438b7ffc712c44f1b8afe"),
    branches={
        b"HEAD": SnapshotBranch(
            target=hash_to_bytes("171dc35522bfd17dda4e90a542a0377fb2fc707a"),
            target_type=TargetType.REVISION,
        )
    },
)


def test_loader_svn_not_found_no_mock(swh_storage, tmp_path):
    """Given an unknown repository, the loader visit ends up in status not_found"""
    repo_url = "unknown-repository"
    loader = SvnLoader(swh_storage, repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage, repo_url, status="not_found", type="svn",
    )


@pytest.mark.parametrize(
    "exception_msg", ["Unable to connect to a repository at URL", "Unknown URL type",]
)
def test_loader_svn_not_found(swh_storage, tmp_path, exception_msg, mocker):
    """Given unknown repository issues, the loader visit ends up in status not_found"""
    mock = mocker.patch("swh.loader.svn.loader.SvnRepo")
    mock.side_effect = SubversionException(exception_msg, 0)

    unknown_repo_url = "unknown-repository"
    loader = SvnLoader(swh_storage, unknown_repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage, unknown_repo_url, status="not_found", type="svn",
    )


@pytest.mark.parametrize(
    "exception",
    [
        SubversionException("Irrelevant message, considered a failure", 10),
        SubversionException("Present but fails to read, considered a failure", 20),
        ValueError("considered a failure"),
    ],
)
def test_loader_svn_failures(swh_storage, tmp_path, exception, mocker):
    """Given any errors raised, the loader visit ends up in status failed"""
    mock = mocker.patch("swh.loader.svn.loader.SvnRepo")
    mock.side_effect = exception

    existing_repo_url = "existing-repo-url"
    loader = SvnLoader(swh_storage, existing_repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        swh_storage, existing_repo_url, status="failed", type="svn",
    )


def test_loader_svnrdump_not_found(swh_storage, tmp_path, mocker):
    """Loading from remote dump which does not exist should end up as not_found visit"""
    unknown_repo_url = "file:///tmp/svn.code.sf.net/p/white-rats-studios/svn"

    loader = SvnLoaderFromRemoteDump(
        swh_storage, unknown_repo_url, destination_path=tmp_path
    )

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage, unknown_repo_url, status="not_found", type="svn",
    )


def test_loader_svnrdump_no_such_revision(swh_storage, tmp_path, datadir):
    """Visit multiple times an origin with the remote loader should not raise.

    It used to fail the ingestion on the second visit with a "No such revision x,
    160006" message.

    """

    archive_dump = os.path.join(datadir, "penguinsdbtools2018.dump.gz")
    loading_path = str(tmp_path / "loading")
    os.mkdir(loading_path)

    # Prepare the dump as a local svn repository for test purposes
    temp_dir, repo_path = init_svn_repo_from_dump(
        archive_dump, root_dir=tmp_path, gzip=True
    )
    repo_url = f"file://{repo_path}"

    loader = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, destination_path=loading_path
    )
    assert loader.load() == {"status": "eventful"}
    actual_visit = assert_last_visit_matches(
        swh_storage, repo_url, status="full", type="svn",
    )

    loader2 = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, destination_path=loading_path
    )
    # Visiting a second time the same repository should be uneventful...
    assert loader2.load() == {"status": "uneventful"}
    actual_visit2 = assert_last_visit_matches(
        swh_storage, repo_url, status="full", type="svn",
    )

    assert actual_visit.snapshot is not None
    # ... with the same snapshot as the first visit
    assert actual_visit2.snapshot == actual_visit.snapshot


def test_loader_svn_new_visit(swh_storage, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 19,
        "directory": 17,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 6,
        "skipped_content": 0,
        "snapshot": 1,
    }

    check_snapshot(GOURMET_SNAPSHOT, loader.storage)


def test_loader_svn_2_visits_no_change(swh_storage, datadir, tmp_path):
    """Visit multiple times a repository with no change should yield the same snapshot

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    assert loader.load() == {"status": "uneventful"}
    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot == visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats["origin_visit"] == 1 + 1  # computed twice the same snapshot
    assert stats["snapshot"] == 1

    # even starting from previous revision...
    start_revision = loader.storage.revision_get(
        [hash_to_bytes("95edacc8848369d6fb1608e887d6d2474fd5224f")]
    )[0]
    assert start_revision is not None

    loader = SvnLoader(swh_storage, repo_url, swh_revision=start_revision)
    assert loader.load() == {"status": "uneventful"}

    stats = get_stats(loader.storage)
    assert stats["origin_visit"] == 2 + 1
    # ... with no change in repository, this yields the same snapshot
    assert stats["snapshot"] == 1

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )


def test_loader_tampered_repository(swh_storage, datadir, tmp_path):
    """In this scenario, the dump has been tampered with to modify the
       commit log [1].  This results in a hash divergence which is
       detected at startup after a new run for the same origin.

       In effect, this will perform a complete reloading of the repository.

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

    loader = SvnLoader(swh_storage, repo_url)
    assert loader.load() == {"status": "eventful"}
    check_snapshot(GOURMET_SNAPSHOT, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    archive_path2 = os.path.join(datadir, "pkg-gourmet-tampered-rev6-log.tgz")
    repo_tampered_url = prepare_repository_from_archive(
        archive_path2, archive_name, tmp_path
    )

    loader2 = SvnLoader(swh_storage, repo_tampered_url, origin_url=repo_url)
    assert loader2.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader2.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=hash_to_bytes("c499eebc1e201024d47d24053ac0080049305897"),
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 2
    assert stats["snapshot"] == 2


def test_loader_svn_visit_with_changes(swh_storage, datadir, tmp_path):
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
    loader = SvnLoader(swh_storage, repo_initial_url)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_initial_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    archive_path = os.path.join(datadir, "pkg-gourmet-with-updates.tgz")
    repo_updated_url = prepare_repository_from_archive(
        archive_path, "pkg-gourmet", tmp_path
    )

    loader = SvnLoader(swh_storage, repo_updated_url, origin_url=repo_initial_url,)

    assert loader.load() == {"status": "eventful"}
    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_updated_url,
        status="full",
        type="svn",
        snapshot=GOURMET_UPDATES_SNAPSHOT.id,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot != visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 22,
        "directory": 28,
        "origin": 1,
        "origin_visit": 2,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 2,
    }

    check_snapshot(GOURMET_UPDATES_SNAPSHOT, loader.storage)

    # Let's start the ingestion from the start, this should yield the same result
    loader = SvnLoader(
        swh_storage, repo_updated_url, origin_url=repo_initial_url, incremental=False,
    )
    assert loader.load() == {"status": "eventful"}
    visit_status3 = assert_last_visit_matches(
        loader.storage,
        repo_updated_url,
        status="full",
        type="svn",
        snapshot=GOURMET_UPDATES_SNAPSHOT.id,
    )
    assert visit_status2.date < visit_status3.date
    assert visit_status3.snapshot == visit_status2.snapshot
    check_snapshot(GOURMET_UPDATES_SNAPSHOT, loader.storage)

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1  # always the same visit
    assert stats["origin_visit"] == 2 + 1  # 1 more visit
    assert stats["snapshot"] == 2  # no new snapshot


def test_loader_svn_visit_start_from_revision(swh_storage, datadir, tmp_path):
    """Starting from existing revision, next visit on changed repo should yield 1 new
       snapshot.

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_initial_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path
    )

    # repo_initial_url becomes the origin_url we want to visit some more below
    loader = SvnLoader(swh_storage, repo_initial_url)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_initial_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    start_revision = loader.storage.revision_get(
        [hash_to_bytes("95edacc8848369d6fb1608e887d6d2474fd5224f")]
    )[0]
    assert start_revision is not None

    archive_path = os.path.join(datadir, "pkg-gourmet-with-updates.tgz")
    repo_updated_url = prepare_repository_from_archive(
        archive_path, "pkg-gourmet", tmp_path
    )

    # we'll start from start_revision
    loader = SvnLoader(
        swh_storage,
        repo_updated_url,
        origin_url=repo_initial_url,
        swh_revision=start_revision,
    )

    assert loader.load() == {"status": "eventful"}

    # nonetheless, we obtain the same snapshot (as previous tests on that repository)
    visit_status2 = assert_last_visit_matches(
        loader.storage,
        repo_updated_url,
        status="full",
        type="svn",
        snapshot=GOURMET_UPDATES_SNAPSHOT.id,
    )

    assert visit_status1.date < visit_status2.date
    assert visit_status1.snapshot != visit_status2.snapshot

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 22,
        "directory": 28,
        "origin": 1,
        "origin_visit": 2,
        "release": 0,
        "revision": 11,
        "skipped_content": 0,
        "snapshot": 2,
    }

    check_snapshot(GOURMET_UPDATES_SNAPSHOT, loader.storage)


def test_loader_svn_visit_with_eol_style(swh_storage, datadir, tmp_path):
    """Check that a svn repo containing a versioned file with CRLF line
       endings with svn:eol-style property set to 'native' (this is a
       violation of svn specification as the file should have been
       stored with LF line endings) can be loaded anyway.

    """
    archive_name = "mediawiki-repo-r407-eol-native-crlf"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    mediawiki_snapshot = Snapshot(
        id=hash_to_bytes("d6d6e9703f157c5702d9a4a5dec878926ed4ab76"),
        branches={
            b"HEAD": SnapshotBranch(
                target=hash_to_bytes("7da4975c363101b819756d33459f30a866d01b1b"),
                target_type=TargetType.REVISION,
            )
        },
    )
    check_snapshot(mediawiki_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=mediawiki_snapshot.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1


def test_loader_svn_visit_with_mixed_crlf_lf(swh_storage, datadir, tmp_path):
    """Check that a svn repo containing a versioned file with mixed
    CRLF/LF line endings with svn:eol-style property set to 'native'
    (this is a violation of svn specification as mixed line endings
    for textual content should not be stored when the svn:eol-style
    property is set) can be loaded anyway.

    """
    archive_name = "pyang-repo-r343-eol-native-mixed-lf-crlf"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    pyang_snapshot = Snapshot(
        id=hash_to_bytes("6d9590de11b00a5801de0ff3297c5b44bbbf7d24"),
        branches={
            b"HEAD": SnapshotBranch(
                target=hash_to_bytes("9c6962eeb9164a636c374be700672355e34a98a7"),
                target_type=TargetType.REVISION,
            )
        },
    )
    check_snapshot(pyang_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn", snapshot=pyang_snapshot.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1


def test_loader_svn_with_external_properties(swh_storage, datadir, tmp_path):
    """Repository with svn:external properties cannot be fully ingested yet

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, "pkg-gourmet-with-external-id.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    gourmet_externals_snapshot = Snapshot(
        id=hash_to_bytes("19cb68d0a3f22372e2b7017ea5e2a2ea5ae3e09a"),
        branches={
            b"HEAD": SnapshotBranch(
                target=hash_to_bytes("82a7a4a09f9549223429143ba36ad77375e33c5c"),
                target_type=TargetType.REVISION,
            )
        },
    )
    check_snapshot(gourmet_externals_snapshot, loader.storage)
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="partial",
        type="svn",
        snapshot=gourmet_externals_snapshot.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    # repository holds 21 revisions, but the last commit holds an 'svn:externals'
    # property which will make the loader-svn stops at the last revision prior to the
    # bad one
    assert stats["revision"] == 21 - 1  # commit with the svn:external property


def test_loader_svn_with_symlink(swh_storage, datadir, tmp_path):
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

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    gourmet_edge_cases_snapshot = Snapshot(
        id=hash_to_bytes("18e60982fe521a2546ab8c3c73a535d80462d9d0"),
        branches={
            b"HEAD": SnapshotBranch(
                target=hash_to_bytes("3f43af2578fccf18b0d4198e48563da7929dc608"),
                target_type=TargetType.REVISION,
            )
        },
    )
    check_snapshot(gourmet_edge_cases_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=gourmet_edge_cases_snapshot.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 19


def test_loader_svn_with_wrong_symlinks(swh_storage, datadir, tmp_path):
    """Repository with wrong symlinks should be ingested ok nonetheless

    Edge case:
       - wrong symbolic link
       - wrong symbolic link with empty space names

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, "pkg-gourmet-with-wrong-link-cases.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    gourmet_wrong_links_snapshot = Snapshot(
        id=hash_to_bytes("b17f38acabb90f066dedd30c29f01a02af88a5c4"),
        branches={
            b"HEAD": SnapshotBranch(
                target=hash_to_bytes("cf30d3bb9d5967d0a2bbeacc405f10a5dd9b138a"),
                target_type=TargetType.REVISION,
            )
        },
    )
    check_snapshot(gourmet_wrong_links_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=gourmet_wrong_links_snapshot.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 21


def test_loader_svn_loader_from_remote_dump(swh_storage, datadir, tmp_path):
    """Repository with wrong symlinks should be ingested ok nonetheless

    Edge case:
       - wrong symbolic link
       - wrong symbolic link with empty space names

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loaderFromDump = SvnLoaderFromRemoteDump(swh_storage, repo_url)
    assert loaderFromDump.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loaderFromDump.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    origin_url = repo_url + "2"  # rename to another origin
    loader = SvnLoader(swh_storage, repo_url, origin_url=origin_url)
    assert loader.load() == {"status": "eventful"}  # because are working on new origin
    assert_last_visit_matches(
        loader.storage,
        origin_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    check_snapshot(GOURMET_SNAPSHOT, loader.storage)

    stats = get_stats(loader.storage)
    assert stats["origin"] == 2  # created one more origin
    assert stats["origin_visit"] == 2
    assert stats["snapshot"] == 1

    loader = SvnLoader(swh_storage, repo_url)  # no change on the origin-url
    assert loader.load() == {"status": "uneventful"}
    assert_last_visit_matches(
        loader.storage,
        origin_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 2
    assert stats["origin_visit"] == 3
    assert stats["snapshot"] == 1

    # second visit from the dump should be uneventful
    loaderFromDump = SvnLoaderFromRemoteDump(swh_storage, repo_url)
    assert loaderFromDump.load() == {"status": "uneventful"}


def test_loader_user_defined_svn_properties(swh_storage, datadir, tmp_path):
    """Edge cases: The repository held some user defined svn-properties with special
       encodings, this prevented the repository from being loaded even though we do not
       ingest those information.

    """
    archive_name = "httthttt"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}
    expected_snapshot = Snapshot(
        id=hash_to_bytes("70487267f682c07e52a2371061369b6cf5bffa47"),
        branches={
            b"HEAD": SnapshotBranch(
                target=hash_to_bytes("604a17dbb15e8d7ecb3e9f3768d09bf493667a93"),
                target_type=TargetType.REVISION,
            )
        },
    )
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=expected_snapshot.id,
    )

    stats = get_stats(loader.storage)
    assert stats["origin"] == 1
    assert stats["origin_visit"] == 1
    assert stats["snapshot"] == 1
    assert stats["revision"] == 7


def test_loader_svn_dir_added_then_removed(swh_storage, datadir, tmp_path):
    """Loader should handle directory removal when processing a commit"""
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}-add-remove-dir.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = SvnLoader(swh_storage, repo_url, destination_path=tmp_path)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )


def test_loader_svn_loader_from_dump_archive(swh_storage, datadir, tmp_path):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    origin_url = f"svn://{archive_name}"
    dump_filename = f"{archive_name}.dump"

    with open(os.path.join(tmp_path, dump_filename), "wb") as dump_file:
        # create compressed dump file of pkg-gourmet repo
        subprocess.run(["svnrdump", "dump", repo_url], stdout=dump_file)
        subprocess.run(["gzip", dump_filename], cwd=tmp_path)

        # load svn repo from that compressed dump file
        loader = SvnLoaderFromDumpArchive(
            swh_storage,
            url=origin_url,
            archive_path=os.path.join(tmp_path, f"{dump_filename}.gz"),
        )

        assert loader.load() == {"status": "eventful"}

        assert_last_visit_matches(
            loader.storage,
            origin_url,
            status="full",
            type="svn",
            snapshot=GOURMET_SNAPSHOT.id,
        )

        check_snapshot(GOURMET_SNAPSHOT, loader.storage)

        assert get_stats(loader.storage) == {
            "content": 19,
            "directory": 17,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 6,
            "skipped_content": 0,
            "snapshot": 1,
        }


class CommitChangeType(Enum):
    AddOrUpdate = 1
    Delete = 2


class CommitChange(TypedDict, total=False):
    change_type: CommitChangeType
    path: str
    properties: Dict[str, str]
    data: bytes


def add_commit(repo_url: str, message: str, changes: List[CommitChange]) -> None:
    conn = RemoteAccess(repo_url, auth=Auth([get_username_provider()]))
    editor = conn.get_commit_editor({"svn:log": message})
    root = editor.open_root()
    for change in changes:
        if change["change_type"] == CommitChangeType.Delete:
            root.delete_entry(change["path"].rstrip("/"))
        else:
            dir_change = change["path"].endswith("/")
            split_path = change["path"].rstrip("/").split("/")
            for i in range(len(split_path)):
                path = "/".join(split_path[0 : i + 1])
                if i < len(split_path) - 1:
                    try:
                        root.add_directory(path).close()
                    except SubversionException:
                        pass
                else:
                    if dir_change:
                        root.add_directory(path).close()
                    else:
                        try:
                            file = root.add_file(path)
                        except SubversionException:
                            file = root.open_file(path)
                        if "properties" in change:
                            for prop, value in change["properties"].items():
                                file.change_prop(prop, value)
                        if "data" in change:
                            txdelta = file.apply_textdelta()
                            delta.send_stream(BytesIO(change["data"]), txdelta)
                        file.close()
    root.close()
    editor.close()


def test_loader_eol_style_file_property_handling_edge_case(swh_storage, tmp_path):
    # create a repository
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"

    # # first commit
    add_commit(
        repo_url,
        (
            "Add a directory containing a file with CRLF end of line "
            "and set svn:eol-style property to native so CRLF will be "
            "replaced by LF in the file when exporting the revision"
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="directory/file_with_crlf_eol.txt",
                properties={"svn:eol-style": "native"},
                data=b"Hello world!\r\n",
            )
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Remove previously added directory and file",
        [CommitChange(change_type=CommitChangeType.Delete, path="directory/",)],
    )

    # third commit
    add_commit(
        repo_url,
        (
            "Add again same directory containing same file with CRLF end of line "
            "but do not set svn:eol-style property value so CRLF will not be "
            "replaced by LF when exporting the revision"
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="directory/file_with_crlf_eol.txt",
                data=b"Hello world!\r\n",
            )
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(
        swh_storage, repo_url, destination_path=tmp_path, check_revision=1
    )

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )

    assert get_stats(loader.storage) == {
        "content": 2,
        "directory": 5,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }


def get_head_revision_paths_info(loader: SvnLoader) -> Dict[bytes, Dict[str, Any]]:
    assert loader.snapshot is not None
    root_dir = loader.snapshot.branches[b"HEAD"].target
    revision = loader.storage.revision_get([root_dir])[0]
    assert revision is not None

    paths = {}
    for entry in loader.storage.directory_ls(revision.directory, recursive=True):
        paths[entry["name"]] = entry
    return paths


def test_loader_eol_style_on_svn_link_handling(swh_storage, tmp_path):
    # create a repository
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"

    # first commit
    add_commit(
        repo_url,
        (
            "Add a regular file, a directory and a link to the regular file "
            "in the directory. Set svn:eol-style property for the regular "
            "file and the link. Set svn:special property for the link."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="file_with_crlf_eol.txt",
                properties={"svn:eol-style": "native"},
                data=b"Hello world!\r\n",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="directory/file_with_crlf_eol.txt",
                properties={"svn:eol-style": "native", "svn:special": "*"},
                data=b"link ../file_with_crlf_eol.txt",
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(
        swh_storage, repo_url, destination_path=tmp_path, check_revision=1
    )

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )

    # check loaded objects are those expected
    assert get_stats(loader.storage) == {
        "content": 2,
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    }

    paths = get_head_revision_paths_info(loader)

    assert (
        loader.storage.content_get_data(paths[b"file_with_crlf_eol.txt"]["sha1"])
        == b"Hello world!\n"
    )

    assert paths[b"directory/file_with_crlf_eol.txt"]["perms"] == DentryPerms.symlink
    assert (
        loader.storage.content_get_data(
            paths[b"directory/file_with_crlf_eol.txt"]["sha1"]
        )
        == b"../file_with_crlf_eol.txt"
    )


def test_loader_svn_special_property_unset(swh_storage, tmp_path):
    # create a repository
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"

    # first commit
    add_commit(
        repo_url,
        (
            "Create a regular file, a link to a file and a link to an "
            "external file. Set the svn:special property on the links."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="file.txt",
                data=b"Hello world!\n",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="link.txt",
                properties={"svn:special": "*"},
                data=b"link ./file.txt",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="external_link.txt",
                properties={"svn:special": "*"},
                data=b"link /home/user/data.txt",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Unset the svn:special property on the links.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="link.txt",
                properties={"svn:special": None},
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="external_link.txt",
                properties={"svn:special": None},
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(
        swh_storage, repo_url, destination_path=tmp_path, check_revision=1
    )

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )

    # check loaded objects are those expected
    assert get_stats(loader.storage) == {
        "content": 5,
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 2,
        "skipped_content": 0,
        "snapshot": 1,
    }

    paths = get_head_revision_paths_info(loader)

    assert paths[b"link.txt"]["perms"] == DentryPerms.content
    assert (
        loader.storage.content_get_data(paths[b"link.txt"]["sha1"])
        == b"link ./file.txt"
    )

    assert paths[b"external_link.txt"]["perms"] == DentryPerms.content
    assert (
        loader.storage.content_get_data(paths[b"external_link.txt"]["sha1"])
        == b"link /home/user/data.txt"
    )


def test_loader_invalid_svn_eol_style_property_value(swh_storage, tmp_path):
    # create a repository
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"

    filename = "file_with_crlf_eol.txt"
    file_content = b"Hello world!\r\n"

    # # first commit
    add_commit(
        repo_url,
        (
            "Add a file with CRLF end of line and set svn:eol-style property "
            "to an invalid value."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path=filename,
                properties={"svn:eol-style": "foo"},
                data=file_content,
            )
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(
        swh_storage, repo_url, destination_path=tmp_path, check_revision=1
    )

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )

    paths = get_head_revision_paths_info(loader)
    # end of lines should not have been processed
    assert (
        loader.storage.content_get_data(paths[filename.encode()]["sha1"])
        == file_content
    )


def test_loader_first_revision_is_not_number_one(swh_storage, mocker, tmp_path):
    class SvnRepoSkipFirstRevision(SvnRepo):
        def logs(self, revision_start, revision_end):
            """Overrides logs method to skip revision number one in yielded revisions"""
            yield from super().logs(revision_start + 1, revision_end)

    from swh.loader.svn import loader

    mocker.patch.object(loader, "SvnRepo", SvnRepoSkipFirstRevision)
    # create a repository
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"

    for filename in ("foo", "bar", "baz"):
        add_commit(
            repo_url,
            f"Add {filename} file",
            [
                CommitChange(
                    change_type=CommitChangeType.AddOrUpdate,
                    path=filename,
                    data=f"{filename}\n".encode(),
                )
            ],
        )

    loader = SvnLoader(swh_storage, repo_url, destination_path=tmp_path)

    # post loading will detect an issue and make a partial visit with a snapshot
    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        loader.storage, repo_url, status="partial", type="svn",
    )

    assert get_stats(loader.storage) == {
        "content": 2,
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 2,
        "skipped_content": 0,
        "snapshot": 1,
    }


def test_loader_svn_special_property_on_binary_file_with_null_byte(
    swh_storage, tmp_path
):
    """When a file has the svn:special property set but is not a svn link,
    it will be truncated when performing an export operation if it contains
    a null byte. Indeed, subversion will treat the file content as text but
    it might be a binary file containing null bytes."""

    # create a repository
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"

    data = (
        b"!<symlink>\xff\xfea\x00p\x00t\x00-\x00c\x00y\x00g\x00.\x00s\x00h\x00\x00\x00"
    )

    # first commit
    add_commit(
        repo_url,
        "Add a non svn link binary file and set the svn:special property on it",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="binary_file",
                properties={"svn:special": "*"},
                data=data,
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Remove the svn:special property on the previously added file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="binary_file",
                properties={"svn:special": None},
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(
        swh_storage, repo_url, destination_path=tmp_path, check_revision=1
    )

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )


def test_loader_last_revision_divergence(swh_storage, datadir, tmp_path):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    class SvnLoaderRevisionDivergence(SvnLoader):
        def _check_revision_divergence(self, count, rev, dir_id):
            raise ValueError("revision divergence detected")

    loader = SvnLoaderRevisionDivergence(
        swh_storage, repo_url, destination_path=tmp_path
    )

    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="partial",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
