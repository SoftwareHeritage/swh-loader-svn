# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum
from io import BytesIO
import os
import shutil
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
from swh.loader.svn.utils import init_svn_repo_from_dump, svn_urljoin
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
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

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
    loader = SvnLoader(swh_storage, unknown_repo_url, temp_directory=tmp_path)

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
    loader = SvnLoader(swh_storage, existing_repo_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        swh_storage, existing_repo_url, status="failed", type="svn",
    )


def test_loader_svnrdump_not_found(swh_storage, tmp_path, mocker):
    """Loading from remote dump which does not exist should end up as not_found visit"""
    unknown_repo_url = "file:///tmp/svn.code.sf.net/p/white-rats-studios/svn"

    loader = SvnLoaderFromRemoteDump(
        swh_storage, unknown_repo_url, temp_directory=tmp_path
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
    archive_ori_dump = os.path.join(datadir, "penguinsdbtools2018.dump.gz")
    archive_dump_dir = os.path.join(tmp_path, "dump")
    os.mkdir(archive_dump_dir)
    archive_dump = os.path.join(archive_dump_dir, "penguinsdbtools2018.dump.gz")
    # loader now drops the dump as soon as it's mounted so we need to make a copy first
    shutil.copyfile(archive_ori_dump, archive_dump)

    loading_path = str(tmp_path / "loading")
    os.mkdir(loading_path)

    # Prepare the dump as a local svn repository for test purposes
    temp_dir, repo_path = init_svn_repo_from_dump(
        archive_dump, root_dir=tmp_path, gzip=True
    )
    repo_url = f"file://{repo_path}"

    loader = SvnLoaderFromRemoteDump(swh_storage, repo_url, temp_directory=loading_path)
    assert loader.load() == {"status": "eventful"}
    actual_visit = assert_last_visit_matches(
        swh_storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    loader2 = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=loading_path
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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(loader.snapshot, loader.storage)

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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(loader.snapshot, loader.storage)

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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)
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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)
    assert loader.load() == {"status": "eventful"}
    check_snapshot(GOURMET_SNAPSHOT, loader.storage)

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(loader.snapshot, loader.storage)

    archive_path2 = os.path.join(datadir, "pkg-gourmet-tampered-rev6-log.tgz")
    repo_tampered_url = prepare_repository_from_archive(
        archive_path2, archive_name, tmp_path
    )

    loader2 = SvnLoader(
        swh_storage, repo_tampered_url, origin_url=repo_url, temp_directory=tmp_path
    )
    assert loader2.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader2.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=hash_to_bytes("5aa61959e788e281fd6e187053d0f46c68e8d8bb"),
    )
    check_snapshot(loader.snapshot, loader.storage)

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
    loader = SvnLoader(swh_storage, repo_initial_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_initial_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(GOURMET_SNAPSHOT, loader.storage)

    archive_path = os.path.join(datadir, "pkg-gourmet-with-updates.tgz")
    repo_updated_url = prepare_repository_from_archive(
        archive_path, "pkg-gourmet", tmp_path
    )

    loader = SvnLoader(
        swh_storage,
        repo_updated_url,
        origin_url=repo_initial_url,
        temp_directory=tmp_path,
    )

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
        swh_storage,
        repo_updated_url,
        origin_url=repo_initial_url,
        incremental=False,
        temp_directory=tmp_path,
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
    loader = SvnLoader(swh_storage, repo_initial_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "eventful"}
    visit_status1 = assert_last_visit_matches(
        loader.storage,
        repo_initial_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(GOURMET_SNAPSHOT, loader.storage)

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
        temp_directory=tmp_path,
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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

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


def test_loader_svn_cleanup_loader(swh_storage, datadir, tmp_path):
    """Loader should clean up its working directory after the load

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loading_temp_directory = str(tmp_path / "loading")
    os.mkdir(loading_temp_directory)
    loader = SvnLoader(swh_storage, repo_url, temp_directory=loading_temp_directory)
    assert loader.load() == {"status": "eventful"}

    # the root temporary directory still exists
    assert os.path.exists(loader.temp_directory)
    # but it should be empty
    assert os.listdir(loader.temp_directory) == []


def test_loader_svn_cleanup_loader_from_remote_dump(swh_storage, datadir, tmp_path):
    """Loader should clean up its working directory after the load

    """
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loading_temp_directory = str(tmp_path / "loading")
    os.mkdir(loading_temp_directory)

    loader = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=loading_temp_directory
    )
    assert loader.load() == {"status": "eventful"}

    # the root temporary directory still exists
    assert os.path.exists(loader.temp_directory)
    # but it should be empty
    assert os.listdir(loader.temp_directory) == []
    # the internal temp_dir should be cleaned up though
    assert not os.path.exists(loader.temp_dir)


def test_loader_svn_cleanup_loader_from_dump_archive(swh_storage, datadir, tmp_path):
    """Loader should clean up its working directory after the load

    """
    archive_ori_dump = os.path.join(datadir, "penguinsdbtools2018.dump.gz")
    archive_dump_dir = os.path.join(tmp_path, "dump")
    os.mkdir(archive_dump_dir)
    archive_dump = os.path.join(archive_dump_dir, "penguinsdbtools2018.dump.gz")
    # loader now drops the dump as soon as it's mounted so we need to make a copy first
    shutil.copyfile(archive_ori_dump, archive_dump)

    loading_path = str(tmp_path / "loading")
    os.mkdir(loading_path)

    # Prepare the dump as a local svn repository for test purposes
    temp_dir, repo_path = init_svn_repo_from_dump(
        archive_dump, root_dir=tmp_path, gzip=True
    )
    repo_url = f"file://{repo_path}"

    loader = SvnLoaderFromRemoteDump(swh_storage, repo_url, temp_directory=loading_path)
    assert loader.load() == {"status": "eventful"}

    # the root temporary directory still exists
    assert os.path.exists(loader.temp_directory)
    # but it should be empty
    assert os.listdir(loader.temp_directory) == []
    # the internal temp_dir should be cleaned up though
    assert not os.path.exists(loader.temp_dir)


def test_svn_loader_from_remote_dump(swh_storage, datadir, tmp_path):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loaderFromDump = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=tmp_path
    )
    assert loaderFromDump.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loaderFromDump.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    origin_url = repo_url + "2"  # rename to another origin
    loader = SvnLoader(
        swh_storage, repo_url, origin_url=origin_url, temp_directory=tmp_path
    )
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

    loader = SvnLoader(
        swh_storage, repo_url, temp_directory=tmp_path
    )  # no change on the origin-url
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
    loaderFromDump = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=tmp_path
    )
    assert loaderFromDump.load() == {"status": "uneventful"}


def test_svn_loader_from_remote_dump_incremental_load_on_stale_repo(
    swh_storage, datadir, tmp_path, mocker
):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    # first load: a dump file will be created, mounted to a local repository
    # and the latter will be loaded into the archive
    loaderFromDump = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=tmp_path
    )
    assert loaderFromDump.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loaderFromDump.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    # second load on same repository: the loader will detect there is no changes
    # since last load and will skip the dump, mount and load phases
    loaderFromDump = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=tmp_path
    )

    loaderFromDump.dump_svn_revisions = mocker.MagicMock()
    init_svn_repo_from_dump = mocker.patch(
        "swh.loader.svn.loader.init_svn_repo_from_dump"
    )
    loaderFromDump.process_svn_revisions = mocker.MagicMock()
    loaderFromDump._check_revision_divergence = mocker.MagicMock()

    assert loaderFromDump.load() == {"status": "uneventful"}
    assert_last_visit_matches(
        loaderFromDump.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )

    # no dump
    loaderFromDump.dump_svn_revisions.assert_not_called()
    # no mount
    init_svn_repo_from_dump.assert_not_called()
    # no loading
    loaderFromDump.process_svn_revisions.assert_not_called()
    # no redundant post_load processing
    loaderFromDump._check_revision_divergence.assert_not_called()


def test_svn_loader_from_remote_dump_incremental_load_on_non_stale_repo(
    swh_storage, datadir, tmp_path, mocker
):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    # first load
    loader = SvnLoaderFromRemoteDump(swh_storage, repo_url, temp_directory=tmp_path)
    loader.load()

    archive_path = os.path.join(datadir, "pkg-gourmet-with-updates.tgz")
    repo_updated_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path
    )

    # second load
    loader = SvnLoaderFromRemoteDump(
        swh_storage, repo_updated_url, temp_directory=tmp_path
    )

    dump_svn_revisions = mocker.spy(loader, "dump_svn_revisions")
    process_svn_revisions = mocker.spy(loader, "process_svn_revisions")

    loader.load()

    dump_svn_revisions.assert_called()
    process_svn_revisions.assert_called()


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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


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
            temp_directory=tmp_path,
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
                        try:
                            dir = root.add_directory(path)
                        except SubversionException:
                            dir = root.open_directory(path)
                        if "properties" in change:
                            for prop, value in change["properties"].items():
                                dir.change_prop(prop, value)
                        dir.close()
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


def create_repo(tmp_path):
    repo_path = os.path.join(tmp_path, "tmprepo")
    repos.create(repo_path)
    repo_url = f"file://{repo_path}"
    return repo_url


@pytest.fixture
def repo_url(tmpdir_factory):
    # create a repository
    return create_repo(tmpdir_factory.mktemp("repos"))


def test_loader_eol_style_file_property_handling_edge_case(
    swh_storage, repo_url, tmp_path
):

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
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

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


def test_loader_eol_style_on_svn_link_handling(swh_storage, repo_url, tmp_path):

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
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

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


def test_loader_svn_special_property_unset(swh_storage, repo_url, tmp_path):

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
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

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


def test_loader_invalid_svn_eol_style_property_value(swh_storage, repo_url, tmp_path):

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
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    paths = get_head_revision_paths_info(loader)
    # end of lines should not have been processed
    assert (
        loader.storage.content_get_data(paths[filename.encode()]["sha1"])
        == file_content
    )


def test_loader_first_revision_is_not_number_one(
    swh_storage, mocker, repo_url, tmp_path
):
    class SvnRepoSkipFirstRevision(SvnRepo):
        def logs(self, revision_start, revision_end):
            """Overrides logs method to skip revision number one in yielded revisions"""
            yield from super().logs(revision_start + 1, revision_end)

    from swh.loader.svn import loader

    mocker.patch.object(loader, "SvnRepo", SvnRepoSkipFirstRevision)

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

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)

    # post loading will detect an issue and make a partial visit with a snapshot
    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        loader.storage, repo_url, status="partial", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

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


def test_loader_svn_special_property_on_binary_file(swh_storage, repo_url, tmp_path):
    """When a file has the svn:special property set but is not a svn link,
    it might be truncated under certain conditions when performing an export
    operation."""

    data = (
        b"!<symlink>\xff\xfea\x00p\x00t\x00-\x00c\x00y\x00g\x00.\x00s\x00h\x00\x00\x00"
    )

    # first commit
    add_commit(
        repo_url,
        (
            "Add a non svn link binary file and set the svn:special property on it."
            "That file will be truncated when exporting it."
        ),
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
        (
            "Add a non svn link binary file and set the svn:special and "
            "svn:mime-type properties on it."
            "That file will not be truncated when exporting it."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="another_binary_file",
                properties={
                    "svn:special": "*",
                    "svn:mime-type": "application/octet-stream",
                },
                data=data,
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Remove the svn:special property on the previously added files",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="binary_file",
                properties={"svn:special": None},
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="another_binary_file",
                properties={"svn:special": None},
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_last_revision_divergence(swh_storage, datadir, tmp_path):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    class SvnLoaderRevisionDivergence(SvnLoader):
        def _check_revision_divergence(self, count, rev, dir_id):
            raise ValueError("revision divergence detected")

    loader = SvnLoaderRevisionDivergence(swh_storage, repo_url, temp_directory=tmp_path)

    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="partial",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(GOURMET_SNAPSHOT, loader.storage)


def test_loader_delete_directory_while_file_has_same_prefix(
    swh_storage, repo_url, tmp_path
):

    # first commit
    add_commit(
        repo_url,
        "Add a file and a directory with same prefix",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="foo/bar.c",
                data=b'#include "../foo.c"',
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="foo.c",
                data=b"int foo() {return 0;}",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Delete previously added directory and update file content",
        [
            CommitChange(change_type=CommitChangeType.Delete, path="foo"),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="foo.c",
                data=b"int foo() {return 1;}",
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_svn_loader_incremental(swh_storage, repo_url, tmp_path):

    # first commit
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
                path="file_with_crlf_eol.txt",
                properties={"svn:eol-style": "native"},
                data=b"Hello world!\r\n",
            )
        ],
    )

    # first load
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    # second commit
    add_commit(
        repo_url,
        "Modify previously added file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="file_with_crlf_eol.txt",
                data=b"Hello World!\r\n",
            )
        ],
    )

    # second load, incremental
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    # third commit
    add_commit(
        repo_url,
        "Unset svn:eol-style property on file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="file_with_crlf_eol.txt",
                properties={"svn:eol-style": None},
            )
        ],
    )

    # third load, incremental
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_svn_loader_incremental_replay_start_with_empty_directory(
    swh_storage, mocker, repo_url, tmp_path
):

    # first commit
    add_commit(
        repo_url,
        ("Add a file"),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate, path="foo.txt", data=b"foo\n",
            )
        ],
    )

    # first load
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    # second commit
    add_commit(
        repo_url,
        "Modify previously added file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate, path="foo.txt", data=b"bar\n",
            )
        ],
    )

    class SvnRepoCheckReplayStartWithEmptyDirectory(SvnRepo):
        def swh_hash_data_per_revision(self, start_revision: int, end_revision: int):
            """Overrides swh_hash_data_per_revision method to grab the content
            of the directory where the svn revisions will be replayed before that
            process starts."""
            self.replay_dir_content_before_start = [
                os.path.join(root, name)
                for root, _, files in os.walk(self.local_url)
                for name in files
            ]
            yield from super().swh_hash_data_per_revision(start_revision, end_revision)

    from swh.loader.svn import loader

    mocker.patch.object(loader, "SvnRepo", SvnRepoCheckReplayStartWithEmptyDirectory)

    # second load, incremental
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path)
    loader.load()

    # check work directory was empty before replaying revisions
    assert loader.svnrepo.replay_dir_content_before_start == []


def test_loader_svn_executable_property_on_svn_link_handling(
    swh_storage, repo_url, tmp_path
):

    # first commit
    add_commit(
        repo_url,
        (
            "Add an executable file and a svn link to it."
            "Set svn:executable property for both paths."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:executable": "*", "svn:special": "*"},
                data=b"link hello-world",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        (
            "Remove executable file, unset link and replace it with executable content."
            "As the link was previously marked as executable, execution rights should"
            "be set after turning it to a regular file."
        ),
        [
            CommitChange(change_type=CommitChangeType.Delete, path="hello-world"),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:special": None},
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_svn_add_property_on_link(swh_storage, repo_url, tmp_path):

    # first commit
    add_commit(
        repo_url,
        "Add an executable file and a svn link to it.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:special": "*"},
                data=b"link hello-world",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set svn:eol-style property on link",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:eol-style": "native"},
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_svn_link_parsing(swh_storage, repo_url, tmp_path):

    # first commit
    add_commit(
        repo_url,
        "Add an executable file and a svn link to it.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:special": "*"},
                data=b"link hello-world",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Update svn link content",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                data=b"link hello-world\r\n",
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_svn_empty_local_dir_before_post_load(swh_storage, datadir, tmp_path):
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    class SvnLoaderPostLoadLocalDirIsEmpty(SvnLoader):
        def post_load(self, success=True):
            if success:
                self.local_dirname_content = [
                    os.path.join(root, name)
                    for root, _, files in os.walk(self.svnrepo.local_dirname)
                    for name in files
                ]
            return super().post_load(success)

    loader = SvnLoaderPostLoadLocalDirIsEmpty(
        swh_storage, repo_url, temp_directory=tmp_path
    )

    assert loader.load() == {"status": "eventful"}

    assert loader.local_dirname_content == []

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
        snapshot=GOURMET_SNAPSHOT.id,
    )
    check_snapshot(GOURMET_SNAPSHOT, loader.storage)


def test_loader_svn_add_property_on_directory_link(swh_storage, repo_url, tmp_path):

    # first commit
    add_commit(
        repo_url,
        "Add an executable file in a directory and a svn link to the directory.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:special": "*"},
                data=b"link code",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set svn:eol-style property on link",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="hello",
                properties={"svn:eol-style": "native"},
            ),
        ],
    )

    # instantiate a svn loader checking after each processed revision that
    # the repository filesystem it reconstructed does not differ from a subversion
    # export of that revision
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)

    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


@pytest.fixture
def external_repo_url(tmpdir_factory):
    # create a repository
    return create_repo(tmpdir_factory.mktemp("external"))


def test_loader_with_valid_svn_externals(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create some directories and files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/hello/hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="foo.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Create repository structure.",
        [
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="branches/",),
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="tags/",),
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="trunk/",),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        (
            "Set svn:externals property on trunk/externals path of repository to load."
            "One external targets a remote directory and another one a remote file."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/hello')} hello\n"
                        f"{svn_urljoin(external_repo_url, 'foo.sh')} foo.sh"
                    )
                },
            ),
        ],
    )

    # first load
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    # third commit
    add_commit(
        repo_url,
        "Unset svn:externals property on trunk/externals path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={"svn:externals": None},
            ),
        ],
    )

    # second load
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_invalid_svn_externals(swh_storage, repo_url, tmp_path):

    # first commit
    add_commit(
        repo_url,
        "Create repository structure.",
        [
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="branches/",),
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="tags/",),
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="trunk/",),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        (
            "Set svn:externals property on trunk/externals path of repository to load."
            "The externals URLs are not valid."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (
                        "file:///tmp/invalid/svn/repo/hello hello\n"
                        "file:///tmp/invalid/svn/repo/foo.sh foo.sh"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_valid_externals_modification(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create some directories and files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/hello/hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/bar/bar.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho bar",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="foo.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        ("Set svn:externals property on trunk/externals path of repository to load."),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/hello')} src/code/hello\n"  # noqa
                        f"{svn_urljoin(external_repo_url, 'foo.sh')} src/foo.sh\n"
                    )
                },
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        (
            "Modify svn:externals property on trunk/externals path of repository to load."  # noqa
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/bar')} src/code/bar\n"  # noqa
                        f"{svn_urljoin(external_repo_url, 'foo.sh')} src/foo.sh\n"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_valid_externals_and_versioned_path(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/script.sh",
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add file with same name but different content in main repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/script.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Add externals targeting the versioned file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/script.sh')} script.sh"  # noqa
                    )
                },
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Modify the versioned file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/script.sh",
                data=b"#!/bin/bash\necho bar",
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_invalid_externals_and_versioned_path(
    swh_storage, repo_url, tmp_path
):

    # first commit
    add_commit(
        repo_url,
        "Add file in main repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/script.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Add invalid externals targeting the versioned file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        "file:///tmp/invalid/svn/repo/code/script.sh script.sh"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_set_externals_then_remove_and_add_as_local(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/script.sh",
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk directory and set externals",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (f"{svn_urljoin(external_repo_url, 'code')} code")
                },
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Unset externals on trunk and add remote path as local path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={"svn:externals": None},
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/code/script.sh",
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_set_invalid_externals_then_remove(swh_storage, repo_url, tmp_path):

    # first commit
    add_commit(
        repo_url,
        "Add trunk directory and set invalid external",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": "file:///tmp/invalid/svn/repo/code external/code"
                },
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Unset externals on trunk",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={"svn:externals": None},
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_set_externals_with_versioned_file_overlap(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/script.sh",
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add file with same name as in the external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/script.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external on trunk overlapping versioned file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/script.sh')} script.sh"
                    )
                },
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Unset externals on trunk",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={"svn:externals": None},
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_dump_loader_relative_externals_detection(
    swh_storage, repo_url, external_repo_url, tmp_path
):

    add_commit(
        external_repo_url,
        "Create a file in external repository.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project1/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    add_commit(
        external_repo_url,
        "Create another file in repository to load.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project2/bar.sh",
                data=b"#!/bin/bash\necho bar",
            ),
        ],
    )

    external_url = f"{external_repo_url.replace('file://', '//')}/project2/bar.sh"
    add_commit(
        repo_url,
        "Set external relative to URL scheme in repository to load",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project1/",
                properties={"svn:externals": (f"{external_url} bar.sh")},
            ),
        ],
    )

    loader = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=tmp_path, check_revision=1
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
    assert loader.svnrepo.has_relative_externals

    add_commit(
        repo_url,
        "Unset external in repository to load",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project1/",
                properties={"svn:externals": None},
            ),
        ],
    )

    loader = SvnLoaderFromRemoteDump(
        swh_storage, repo_url, temp_directory=tmp_path, check_revision=1
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
    assert not loader.svnrepo.has_relative_externals


def test_loader_externals_cache(swh_storage, repo_url, external_repo_url, tmp_path):

    # first commit on external
    add_commit(
        external_repo_url,
        "Create some directories and files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/hello/hello-world",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Create repository structure.",
        [
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="project1/",),
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="project2/",),
        ],
    )

    external_url = svn_urljoin(external_repo_url, "code/hello")

    # second commit
    add_commit(
        repo_url,
        (
            "Set svn:externals property on trunk/externals path of repository to load."
            "One external targets a remote directory and another one a remote file."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project1/externals/",
                properties={"svn:externals": (f"{external_url} hello\n")},
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project2/externals/",
                properties={"svn:externals": (f"{external_url} hello\n")},
            ),
        ],
    )

    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    assert (external_url, None) in loader.svnrepo.swhreplay.editor.externals_cache


def test_loader_remove_versioned_path_with_external_overlap(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/hello.sh",
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add a file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/project/script.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external on trunk overlapping versioned path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code')} project/code"
                    )
                },
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Remove trunk/project/ versioned path",
        [CommitChange(change_type=CommitChangeType.Delete, path="trunk/project/",),],
    )

    loader = SvnLoader(
        swh_storage, repo_url, temp_directory=tmp_path, check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage, repo_url, status="full", type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
