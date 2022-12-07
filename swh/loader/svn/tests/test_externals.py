# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.loader.svn.loader import SvnLoader, SvnLoaderFromRemoteDump
from swh.loader.svn.utils import svn_urljoin
from swh.loader.tests import assert_last_visit_matches, check_snapshot

from .utils import CommitChange, CommitChangeType, add_commit, create_repo


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
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="branches/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="tags/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/bar.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho bar",
            ),
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
                        f"{svn_urljoin(external_repo_url, 'foo.sh')} foo.sh\n"
                        f"{svn_urljoin(repo_url, 'trunk/bar.sh')} bar.sh"
                    )
                },
            ),
        ],
    )

    # first load
    loader = SvnLoader(swh_storage, repo_url, temp_directory=tmp_path, check_revision=1)
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_invalid_svn_externals(swh_storage, repo_url, tmp_path, mocker):

    # first commit
    add_commit(
        repo_url,
        "Create repository structure.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="branches/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="tags/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
            ),
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
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
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project1/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project2/",
            ),
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
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)

    assert (
        external_url,
        None,
        False,
    ) in loader.svnrepo.swhreplay.editor.externals_cache


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
        [
            CommitChange(
                change_type=CommitChangeType.Delete,
                path="trunk/project/",
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_export_external_path_using_peg_rev(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # second commit on external
    add_commit(
        external_repo_url,
        "Remove previously added file",
        [
            CommitChange(
                change_type=CommitChangeType.Delete,
                path="code/foo.sh",
            ),
        ],
    )

    # third commit on external
    add_commit(
        external_repo_url,
        "Add file again but with different content",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                data=b"#!/bin/bash\necho bar",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk dir",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external on trunk targeting first revision of external repo",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/foo.sh')}@1 foo.sh"
                    )
                },
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Modify external on trunk to target third revision of external repo",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/foo.sh')}@3 foo.sh"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_remove_external_overlapping_versioned_path(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/link",
                data=b"#!/bin/bash\necho link",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk dir and a link file",
        [
            CommitChange(change_type=CommitChangeType.AddOrUpdate, path="trunk/"),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/link",
                data=b"link ../test",
                properties={"svn:special": "*"},
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external on root dir overlapping versioned trunk path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="",  # repo root dir
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/foo.sh')} trunk/code/foo.sh\n"  # noqa
                        f"{svn_urljoin(external_repo_url, 'code/link')} trunk/link"
                    )
                },
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Remove external on root dir",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="",
                properties={"svn:externals": None},
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_modify_external_same_path(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk dir",
        [CommitChange(change_type=CommitChangeType.AddOrUpdate, path="trunk/")],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external code on trunk dir",
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

    # third commit
    add_commit(
        repo_url,
        "Change code external on trunk targeting an invalid URL",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={"svn:externals": "file:///tmp/invalid/svn/repo/path code"},
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_recursive_external(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk dir and a file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/bar.sh",
                data=b"#!/bin/bash\necho bar",
            )
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set externals code on trunk/externals dir, one being recursive",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code')} code\n"
                        f"{repo_url} recursive"
                    )
                },
            ),
        ],
    )

    # first load
    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
    assert loader.svnrepo.has_recursive_externals

    # second load on stale repo
    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "uneventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
    assert loader.svnrepo.has_recursive_externals

    # third commit
    add_commit(
        repo_url,
        "Remove recursive external on trunk/externals dir",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (f"{svn_urljoin(external_repo_url, 'code')} code")
                },
            ),
        ],
    )

    # third load
    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
    assert not loader.svnrepo.has_recursive_externals


def test_loader_externals_with_same_target(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="foo/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="bar/bar.sh",
                data=b"#!/bin/bash\necho bar",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk/src dir",
        [CommitChange(change_type=CommitChangeType.AddOrUpdate, path="trunk/src/")],
    )

    # second commit
    add_commit(
        repo_url,
        "Add externals on trunk targeting same directory",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'foo')} src\n"
                        f"{svn_urljoin(external_repo_url, 'bar')} src"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_external_in_versioned_path(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add trunk/src dir",
        [CommitChange(change_type=CommitChangeType.AddOrUpdate, path="trunk/src/")],
    )

    # second commit
    add_commit(
        repo_url,
        "Add a file in trunk/src directory and set external on trunk targeting src",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/bar.sh",
                data=b"#!/bin/bash\necho bar",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (f"{svn_urljoin(external_repo_url, 'src')} src")
                },
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_dump_loader_externals_in_loaded_repository(swh_storage, tmp_path, mocker):
    repo_url = create_repo(tmp_path, repo_name="foo")
    externa_url = create_repo(tmp_path, repo_name="foobar")

    # first commit on external
    add_commit(
        externa_url,
        "Create a file in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    add_commit(
        repo_url,
        (
            "Add a file and set externals on trunk/externals:"
            "one external located in this repository, the other in a remote one"
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/bar.sh",
                data=b"#!/bin/bash\necho bar",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(repo_url, 'trunk/src/bar.sh')} bar.sh\n"
                        f"{svn_urljoin(externa_url, 'trunk/src/foo.sh')} foo.sh"
                    )
                },
            ),
        ],
    )

    from swh.loader.svn.svn import client

    mock_client = mocker.MagicMock()
    mocker.patch.object(client, "Client", mock_client)

    class Info:
        repos_root_url = repo_url

    mock_client().info.return_value = {"repo": Info()}

    loader = SvnLoaderFromRemoteDump(swh_storage, repo_url, temp_directory=tmp_path)
    loader.load()

    export_call_args = mock_client().export.call_args_list

    # first external export should use the base URL of the local repository
    # mounted from the remote dump as it is located in loaded repository
    assert export_call_args[0][0][0] != svn_urljoin(
        loader.svnrepo.origin_url, "trunk/src/bar.sh"
    )
    assert export_call_args[0][0][0] == svn_urljoin(
        loader.svnrepo.remote_url, "trunk/src/bar.sh"
    )

    # second external export should use the remote URL of the external repository
    assert export_call_args[1][0][0] == svn_urljoin(externa_url, "trunk/src/foo.sh")


def test_loader_externals_add_remove_readd_on_subpath(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/foo.sh",
                data=b"#!/bin/bash\necho foo",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/bar.sh",
                data=b"#!/bin/bash\necho bar",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Set external on two paths targeting the same absolute path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'src/foo.sh')} foo.sh"
                    )
                },
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'src/foo.sh')} src/foo.sh"
                    )
                },
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Remove external on a single path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'src/bar.sh')} src/bar.sh"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_directory_symlink_in_external(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create dirs in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/apps/",
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/deps/",
            ),
        ],
    )

    # second commit on external
    add_commit(
        external_repo_url,
        "Add symlink to src/deps in src/apps directory",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/apps/deps",
                data=b"link ../deps",
                properties={"svn:special": "*"},
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Add deps dir",
        [CommitChange(change_type=CommitChangeType.AddOrUpdate, path="deps/")],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external to deps folder",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="deps/",
                properties={"svn:externals": (f"{external_repo_url} external")},
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_externals_parsing_error(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    # first commit on external
    add_commit(
        external_repo_url,
        "Create code directory",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/",
            ),
        ],
    )

    # second commit on external
    add_commit(
        external_repo_url,
        "Create code/foo.sh file",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        "Create trunk directory.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        "Set external on trunk directory that will result in a parsing error.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"-r2{svn_urljoin(external_repo_url, 'code/foo.sh')} foo.sh"
                    )
                },
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        "Fix external definition on trunk directory.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
                properties={
                    "svn:externals": (
                        f"-r2 {svn_urljoin(external_repo_url, 'code/foo.sh')} foo.sh"
                    )
                },
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


@pytest.mark.parametrize("remote_external_path", ["src/main/project", "src/main"])
def test_loader_overlapping_external_paths_removal(
    swh_storage, repo_url, external_repo_url, tmp_path, remote_external_path
):
    add_commit(
        external_repo_url,
        "Create external repository layout",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="src/main/project/foo/bar",
                data=b"bar",
            ),
        ],
    )

    add_commit(
        repo_url,
        "Create repository layout",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/main/project/",
            ),
        ],
    )

    add_commit(
        repo_url,
        "Add overlapping externals",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/main/",
                properties={
                    "svn:externals": f"{svn_urljoin(external_repo_url, remote_external_path)} project"  # noqa
                },
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/src/main/project/",
                properties={
                    "svn:externals": f'{svn_urljoin(external_repo_url, "src/main/project/foo")} foo'  # noqa
                },
            ),
        ],
    )

    add_commit(
        repo_url,
        "Remove directory with externals overlapping with those from ancestor directory",
        [
            CommitChange(
                change_type=CommitChangeType.Delete,
                path="trunk/src/main/project/",
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_copyfrom_rev_with_externals(
    swh_storage, repo_url, external_repo_url, tmp_path
):
    add_commit(
        external_repo_url,
        "Create some directories and files in an external repository",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/hello/hello-world",
                data=b"#!/bin/bash\necho Hello World !",
            ),
        ],
    )

    add_commit(
        repo_url,
        "Create repository structure, one externals directory with svn:externals"
        "property set and one trunk directory",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="externals/",
                properties={
                    "svn:externals": f'{svn_urljoin(external_repo_url, "code/hello/")} hello'  # noqa
                },
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/",
            ),
        ],
    )

    add_commit(
        repo_url,
        "Add copy of externals directory to trunk from revision 1.",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                copyfrom_path=repo_url + "/externals",
                copyfrom_rev=1,
            ),
        ],
    )

    add_commit(
        repo_url,
        "Unset svn:externals property on copied path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/externals/",
                properties={"svn:externals": None},
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)


def test_loader_with_unparsable_external_on_path(
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
                path="code/foo/foo.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # first commit
    add_commit(
        repo_url,
        (
            "Set parsable svn:externals property on project1 path of repository to load."
            "Add a code directory with a file in it."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project1/",
                properties={
                    "svn:externals": (
                        f"{svn_urljoin(external_repo_url, 'code/hello')} hello\n"
                    )
                },
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="code/foo.sh",
                properties={"svn:executable": "*"},
                data=b"#!/bin/bash\necho foo",
            ),
        ],
    )

    # second commit
    add_commit(
        repo_url,
        (
            "Set unparsable svn:externals property on project2 path of repository to load."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project2/",
                properties={"svn:externals": ("^code/foo foo\n")},
            ),
        ],
    )

    # third commit
    add_commit(
        repo_url,
        (
            "Fix unparsable svn:externals property on project2 path of repository to load."
        ),
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="project2/",
                properties={"svn:externals": ("^/code/foo foo\n")},
            ),
        ],
    )

    loader = SvnLoader(
        swh_storage,
        repo_url,
        temp_directory=tmp_path,
        check_revision=1,
    )
    assert loader.load() == {"status": "eventful"}
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="svn",
    )
    check_snapshot(loader.snapshot, loader.storage)
