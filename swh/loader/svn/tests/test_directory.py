# Copyright (C) 2023-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path

import pytest

from swh.core.nar import Nar
from swh.loader.svn.directory import SvnExportLoader
from swh.loader.svn.svn_repo import get_svn_repo
from swh.loader.tests import (
    assert_last_visit_matches,
    fetch_extids_from_checksums,
    get_stats,
    prepare_repository_from_archive,
)


def compute_nar_hash_for_rev(repo_url: str, rev: int, hash_name: str = "sha256") -> str:
    """Compute the Nar hashes of the svn tree at the revision 'rev'."""
    svn_repo = get_svn_repo(repo_url, revision=rev)
    assert svn_repo is not None
    _, export_dir = svn_repo.export_temporary(rev)

    nar = Nar(hash_names=[hash_name])
    nar.serialize(Path(export_dir.decode()))
    return nar.hexdigest()[hash_name]


def test_loader_svn_directory(swh_storage, datadir, tmp_path):
    """Loading a svn tree with proper nar checksums should be eventful"""
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )
    svn_revision = 5
    checksums = {"sha256": compute_nar_hash_for_rev(repo_url, svn_revision)}

    loader = SvnExportLoader(
        swh_storage,
        repo_url,
        ref=svn_revision,
        checksum_layout="nar",
        checksums=checksums,
    )

    actual_result = loader.load()

    assert actual_result == {"status": "eventful"}

    actual_visit = assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="full",
        type="svn-export",
    )

    snapshot = swh_storage.snapshot_get(actual_visit.snapshot)
    assert snapshot is not None

    branches = snapshot["branches"].keys()
    expected_branch = f"rev_{svn_revision}".encode()
    assert set(branches) == {b"HEAD", expected_branch}

    assert get_stats(swh_storage) == {
        "content": 18,
        "directory": 6,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 1,
    }

    # Ensure the extids got stored as well
    extids = fetch_extids_from_checksums(
        loader.storage,
        checksum_layout="nar",
        checksums=checksums,
        extid_version=loader.extid_version,
    )
    assert len(extids) == len(checksums)

    # Another run on the same svn directory should be uneventful
    loader2 = SvnExportLoader(
        swh_storage,
        repo_url,
        ref=svn_revision,
        checksum_layout="nar",
        checksums=checksums,
    )
    actual_result2 = loader2.load()
    assert actual_result2 == {"status": "uneventful"}


@pytest.mark.parametrize(
    "use_custom_origin_url",
    [False, True],
    ids=["origin_url == svn_url", "origin_url != svn_url"],
)
def test_loader_svn_directory_sub_paths(
    swh_storage, datadir, tmp_path, use_custom_origin_url
):
    """Loading a subset of a svn tree with proper nar checksums should be eventful"""
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )
    svn_paths = ["gourmet/trunk/debian/gourmet.1", "gourmet/trunk/debian/patches"]
    svn_revision = 5
    checksum_layout = "nar"
    checksums = {
        "sha256": "21e9553da2f8ae27d6b9ae87f509b0233fc6edbabc1099c31b90e1dec2cbb618"
    }

    origin_url = (
        f"{repo_url}?nar=sha256-{checksums['sha256']}"
        if use_custom_origin_url
        else repo_url
    )
    svn_url = repo_url if use_custom_origin_url else None

    loader = SvnExportLoader(
        swh_storage,
        origin_url,
        svn_url=svn_url,
        ref=svn_revision,
        svn_paths=svn_paths,
        checksum_layout=checksum_layout,
        checksums=checksums,
    )

    actual_result = loader.load()

    assert actual_result == {"status": "eventful"}

    actual_visit = assert_last_visit_matches(
        swh_storage,
        origin_url,
        status="full",
        type="svn-export",
    )

    snapshot = swh_storage.snapshot_get(actual_visit.snapshot)
    assert snapshot is not None

    branches = snapshot["branches"].keys()
    expected_branch = f"rev_{svn_revision}".encode()
    assert set(branches) == {b"HEAD", expected_branch}

    assert get_stats(swh_storage) == {
        "content": 3,
        "directory": 5,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 1,
    }

    # Ensure the extids got stored as well
    extids = fetch_extids_from_checksums(
        loader.storage,
        checksum_layout=checksum_layout,
        checksums=checksums,
        extid_version=loader.extid_version,
    )
    assert extids[0].extid.hex() == checksums["sha256"]

    # Another run should be uneventful
    loader2 = SvnExportLoader(
        swh_storage,
        origin_url,
        svn_url=svn_url,
        ref=svn_revision,
        svn_paths=svn_paths,
        checksum_layout=checksum_layout,
        checksums=checksums,
    )
    actual_result2 = loader2.load()
    assert actual_result2 == {"status": "uneventful"}


def test_loader_svn_directory_hash_mismatch(swh_storage, datadir, tmp_path):
    """Loading a svn tree with faulty checksums should fail"""
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )
    faulty_checksums = {
        "sha256": "00000ed1855beadfa9c00f730242f5efe3e4612e76f0dcc45215c4a3234c7466"
    }
    loader = SvnExportLoader(
        swh_storage,
        repo_url,
        ref=5,
        checksum_layout="nar",
        checksums=faulty_checksums,
    )

    actual_result = loader.load()

    # Ingestion fails because the checks failed
    assert actual_result["status"] == "failed"
    assert get_stats(swh_storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }

    # Ensure no extids got stored
    extids = fetch_extids_from_checksums(
        loader.storage,
        checksum_layout="nar",
        checksums=faulty_checksums,
        extid_version=loader.extid_version,
    )
    assert len(extids) == 0


def test_loader_svn_directory_not_found(swh_storage, datadir, tmp_path):
    """Loading a svn tree from an unknown origin should fail"""
    loader = SvnExportLoader(
        swh_storage,
        "file:///home/origin/does/not/exist",
        ref=5,
        checksum_layout="standard",
        checksums={},
    )

    actual_result = loader.load()

    # Ingestion fails because the checks failed
    assert actual_result == {"status": "uneventful"}
    assert get_stats(swh_storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }


def test_loader_svn_directory_path_not_in_head_revision(swh_storage, datadir, tmp_path):
    """Check a directory path present in specific revision but not present in HEAD revision
    can be exported."""
    archive_name = "pkg-gourmet-add-remove-dir"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = (
        prepare_repository_from_archive(archive_path, "pkg-gourmet", tmp_path=tmp_path)
        # path deleted in HEAD (revision 7)
        + "/gourmet/trunk/debian"
    )
    svn_revision = 6
    checksums = {"sha256": compute_nar_hash_for_rev(repo_url, svn_revision)}

    loader = SvnExportLoader(
        swh_storage,
        repo_url,
        ref=svn_revision,
        checksum_layout="nar",
        checksums=checksums,
    )

    actual_result = loader.load()

    assert actual_result == {"status": "eventful"}
