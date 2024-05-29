# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import pty
import re
import shutil
from subprocess import Popen, run

import pytest

from swh.loader.svn import utils
from swh.loader.tests import prepare_repository_from_archive

from .utils import CommitChange, CommitChangeType, add_commit


def test_outputstream():
    stdout_r, stdout_w = pty.openpty()
    echo = Popen(["echo", "-e", "foo\nbar\nbaz"], stdout=stdout_w)
    os.close(stdout_w)
    stdout_stream = utils.OutputStream(stdout_r)
    lines = []
    while True:
        current_lines, readable = stdout_stream.read_lines()
        lines += current_lines
        if not readable:
            break
    echo.wait()
    os.close(stdout_r)
    assert lines == ["foo", "bar", "baz"]


def test_init_svn_repo_from_dump(datadir, tmp_path):
    """Mounting svn repository out of a dump is ok"""
    dump_name = "penguinsdbtools2018.dump.gz"
    dump_path = os.path.join(datadir, dump_name)

    tmp_repo, repo_path = utils.init_svn_repo_from_dump(
        dump_path, gzip=True, cleanup_dump=False, root_dir=tmp_path
    )

    assert os.path.exists(dump_path), "Dump path should still exists"
    assert os.path.exists(repo_path), "Repository should exists"


def test_init_svn_repo_from_dump_svnadmin_error(tmp_path):
    """svnadmin load error should be reported in exception text"""
    dump_path = os.path.join(tmp_path, "foo")
    Path(dump_path).touch()

    with pytest.raises(
        ValueError,
        match="svnadmin: E200003: Premature end of content data in dumpstream",
    ):
        utils.init_svn_repo_from_dump(dump_path, cleanup_dump=False, root_dir=tmp_path)


def test_init_svn_repo_from_dump_and_cleanup(datadir, tmp_path):
    """Mounting svn repository with a dump cleanup after is ok"""
    dump_name = "penguinsdbtools2018.dump.gz"
    dump_ori_path = os.path.join(datadir, dump_name)

    dump_path = os.path.join(tmp_path, dump_name)
    shutil.copyfile(dump_ori_path, dump_path)

    assert os.path.exists(dump_path)
    assert os.path.exists(dump_ori_path)

    tmp_repo, repo_path = utils.init_svn_repo_from_dump(
        dump_path, gzip=True, root_dir=tmp_path
    )

    assert not os.path.exists(dump_path), "Dump path should no longer exists"
    assert os.path.exists(repo_path), "Repository should exists"
    assert os.path.exists(dump_ori_path), "Original dump path should still exists"


def test_init_svn_repo_from_dump_and_cleanup_already_done(
    datadir, tmp_path, mocker, caplog
):
    """Mounting svn repository out of a dump is ok"""
    caplog.set_level(logging.INFO, "swh.loader.svn.utils")

    dump_name = "penguinsdbtools2018.dump.gz"
    dump_ori_path = os.path.join(datadir, dump_name)

    mock_remove = mocker.patch("os.remove")
    mock_remove.side_effect = FileNotFoundError

    dump_path = os.path.join(tmp_path, dump_name)
    shutil.copyfile(dump_ori_path, dump_path)

    assert os.path.exists(dump_path)
    assert os.path.exists(dump_ori_path)

    tmp_repo, repo_path = utils.init_svn_repo_from_dump(
        dump_path, gzip=True, root_dir=tmp_path
    )

    assert os.path.exists(repo_path), "Repository should exists"
    assert os.path.exists(dump_ori_path), "Original dump path should still exists"

    assert len(caplog.record_tuples) == 1
    assert "Failure to remove" in caplog.record_tuples[0][2]
    assert mock_remove.called


def test_init_svn_repo_from_truncated_dump(datadir, tmp_path):
    """Mounting partial svn repository from a truncated dump should work"""

    # prepare a repository
    archive_name = "pkg-gourmet"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    # dump it to file
    dump_path = str(tmp_path / f"{archive_name}.dump")
    truncated_dump_path = str(tmp_path / f"{archive_name}_truncated.dump")
    svnrdump_cmd = ["svnrdump", "dump", repo_url]
    with open(dump_path, "wb") as dump:
        run(svnrdump_cmd, stdout=dump)

    # create a truncated dump file that will generate a "svnadmin load" error
    with (
        open(dump_path, "rb") as dump,
        open(truncated_dump_path, "wb") as truncated_dump,
    ):
        dump_lines = dump.readlines()
        assert len(dump_lines) > 150
        truncated_dump_content = b"".join(dump_lines[:150])
        truncated_dump.write(truncated_dump_content)

        # compute max revision number with non truncated data
        revs = re.findall(rb"Revision-number: ([0-9]+)", truncated_dump_content)
        max_rev = int(revs[-1]) - 1

    # prepare repository from truncated dump
    _, repo_path = utils.init_svn_repo_from_dump(
        truncated_dump_path, gzip=False, root_dir=tmp_path, max_rev=max_rev
    )

    # check expected number of revisions have been loaded
    svnadmin_info = run(["svnadmin", "info", repo_path], capture_output=True, text=True)
    assert f"Revisions: {max_rev}\n" in svnadmin_info.stdout


def test_init_svn_repo_from_gzip_dump(datadir, tmp_path):
    """Mounting svn repository out of an archive dump is ok"""
    dump_name = "penguinsdbtools2018.dump.gz"
    dump_path = os.path.join(datadir, dump_name)

    tmp_repo, repo_path = utils.init_svn_repo_from_dump(
        dump_path,
        cleanup_dump=False,
        root_dir=tmp_path,
        gzip=True,
    )

    assert os.path.exists(dump_path), "Dump path should still exists"
    assert os.path.exists(repo_path), "Repository should exists"


def test_init_svn_repo_from_gzip_dump_and_cleanup(datadir, tmp_path):
    """Mounting svn repository out of a dump is ok"""
    dump_name = "penguinsdbtools2018.dump.gz"
    dump_ori_path = os.path.join(datadir, dump_name)

    dump_path = os.path.join(tmp_path, dump_name)
    shutil.copyfile(dump_ori_path, dump_path)

    assert os.path.exists(dump_path)
    assert os.path.exists(dump_ori_path)

    tmp_repo, repo_path = utils.init_svn_repo_from_dump(
        dump_path,
        root_dir=tmp_path,
        gzip=True,
    )

    assert not os.path.exists(dump_path), "Dump path should no longer exists"
    assert os.path.exists(repo_path), "Repository should exists"
    assert os.path.exists(dump_ori_path), "Original dump path should still exists"


@pytest.mark.parametrize(
    "base_url, paths_to_join, expected_result",
    [
        (
            "https://svn.example.org",
            ["repos", "test"],
            "https://svn.example.org/repos/test",
        ),
        (
            "https://svn.example.org/",
            ["repos", "test"],
            "https://svn.example.org/repos/test",
        ),
        (
            "https://svn.example.org/foo",
            ["repos", "test"],
            "https://svn.example.org/foo/repos/test",
        ),
        (
            "https://svn.example.org/foo/",
            ["/repos", "test/"],
            "https://svn.example.org/foo/repos/test",
        ),
        (
            "https://svn.example.org/foo",
            ["../bar"],
            "https://svn.example.org/bar",
        ),
    ],
)
def test_svn_urljoin(base_url, paths_to_join, expected_result):
    assert utils.svn_urljoin(base_url, *paths_to_join) == expected_result


parse_external_test_params = [
    # subversion < 1.5
    (
        "third-party/sounds             http://svn.example.com/repos/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.com/repos/sounds",
            None,
            None,
            False,
            True,
        ),
    ),
    (
        "third-party/skins -r148        http://svn.example.com/skinproj",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins",
            "http://svn.example.com/skinproj",
            148,
            None,
            False,
            True,
        ),
    ),
    (
        "third-party/skins/toolkit -r21 http://svn.example.com/skin-maker",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://svn.example.com/skin-maker",
            21,
            None,
            False,
            True,
        ),
    ),
    # subversion >= 1.5
    (
        "      http://svn.example.com/repos/sounds third-party/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.com/repos/sounds",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "-r148 http://svn.example.com/skinproj third-party/skins",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins",
            "http://svn.example.com/skinproj",
            148,
            None,
            False,
            False,
        ),
    ),
    (
        "-r 21 http://svn.example.com/skin-maker third-party/skins/toolkit",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://svn.example.com/skin-maker",
            21,
            None,
            False,
            False,
        ),
    ),
    (
        "http://svn.example.com/repos/sounds third-party/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.com/repos/sounds",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "http://svn.example.com/skinproj@148 third-party/skins",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins",
            "http://svn.example.com/skinproj",
            None,
            148,
            False,
            False,
        ),
    ),
    (
        "http://anon:anon@svn.example.com/skin-maker@21 third-party/skins/toolkit",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://anon:anon@svn.example.com/skin-maker",
            None,
            21,
            False,
            False,
        ),
    ),
    (
        "-r21 http://anon:anon@svn.example.com/skin-maker third-party/skins/toolkit",  # noqa
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://anon:anon@svn.example.com/skin-maker",
            21,
            None,
            False,
            False,
        ),
    ),
    (
        "-r21 http://anon:anon@svn.example.com/skin-maker@21 third-party/skins/toolkit",  # noqa
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://anon:anon@svn.example.com/skin-maker",
            21,
            21,
            False,
            False,
        ),
    ),
    # subversion >= 1.5, relative external definitions
    (
        "^/sounds third-party/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.org/repos/test/sounds",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "/skinproj@148 third-party/skins",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins",
            "http://svn.example.org/skinproj",
            None,
            148,
            True,
            False,
        ),
    ),
    (
        "//svn.example.com/skin-maker@21 third-party/skins/toolkit",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://svn.example.com/skin-maker",
            None,
            21,
            True,
            False,
        ),
    ),
    (
        "^/../../skin-maker@21 third-party/skins/toolkit",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/skins/toolkit",
            "http://svn.example.org/skin-maker",
            None,
            21,
            True,
            False,
        ),
    ),
    (
        "../skins skins",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "skins",
            "http://svn.example.org/repos/test/trunk/skins",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "../skins skins",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "skins",
            "http://svn.example.org/repos/test/trunk/skins",
            None,
            None,
            False,
            False,
        ),
    ),
    # subversion >= 1.6
    (
        'http://example.com/svn/repos/My%20Project "My Project"',
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "My Project",
            "http://example.com/svn/repos/My Project",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "http://example.com/svn/repos/My%20Project 'My Project'",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "My Project",
            "http://example.com/svn/repos/My Project",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        'http://example.com/svn/repos/My%20%20%20Project "My   Project"',
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "My   Project",
            "http://example.com/svn/repos/My   Project",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        'http://example.com/svn/repos/%22Quotes%20Too%22 \\"Quotes\\ Too\\"',
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            '"Quotes Too"',
            'http://example.com/svn/repos/"Quotes Too"',
            None,
            None,
            False,
            False,
        ),
    ),
    (
        'http://example.com/svn/repos/%22Quotes%20%20%20Too%22 \\"Quotes\\ \\ \\ Too\\"',  # noqa
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            '"Quotes   Too"',
            'http://example.com/svn/repos/"Quotes   Too"',
            None,
            None,
            False,
            False,
        ),
    ),
    # edge cases
    (
        '-r1 http://example.com/svn/repos/test "trunk/PluginFramework"',
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "trunk/PluginFramework",
            "http://example.com/svn/repos/test",
            1,
            None,
            False,
            False,
        ),
    ),
    (
        "external -r 9 http://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "external",
            "http://example.com/svn/repos/test",
            9,
            None,
            False,
            True,
        ),
    ),
    (
        "./external http://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "external",
            "http://example.com/svn/repos/test",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        ".external http://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            ".external",
            "http://example.com/svn/repos/test",
            None,
            None,
            False,
            True,
        ),
    ),
    (
        "external/ http://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "external",
            "http://example.com/svn/repos/test",
            None,
            None,
            False,
            True,
        ),
    ),
    (
        "external ttp://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "external",
            "ttp://example.com/svn/repos/test",
            None,
            None,
            False,
            True,
        ),
    ),
    (
        "external http//example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "external",
            "http//example.com/svn/repos/test",
            None,
            None,
            False,
            True,
        ),
    ),
    (
        "C:\\code\\repo\\external http://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "C:coderepoexternal",
            "http://example.com/svn/repos/test",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "C:\\\\code\\\\repo\\\\external http://example.com/svn/repos/test",
        "tags",
        "http://svn.example.org/repos/test",
        (
            "C:\\code\\repo\\external",
            "http://example.com/svn/repos/test",
            None,
            None,
            False,
            False,
        ),
    ),
    (
        "-r 123 http://svn.example.com/repos/sounds@100 third-party/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.com/repos/sounds",
            123,
            100,
            False,
            False,
        ),
    ),
    (
        "-r 123 http://svn.example.com/repos/sounds@150 third-party/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.com/repos/sounds",
            123,
            150,
            False,
            False,
        ),
    ),
    (
        "-r 123 http://svn.example.com/repos/some%20sounds@150 third-party/sounds",
        "trunk/externals",
        "http://svn.example.org/repos/test",
        (
            "third-party/sounds",
            "http://svn.example.com/repos/some sounds",
            123,
            150,
            False,
            False,
        ),
    ),
]


@pytest.mark.parametrize(
    "external, dir_path, repo_url, expected_result",
    parse_external_test_params,
    ids=[args[0] for args in parse_external_test_params],
)
def test_parse_external_definition(external, dir_path, repo_url, expected_result):
    assert utils.parse_external_definition(
        external, dir_path, repo_url
    ) == utils.ExternalDefinition(*expected_result)


@pytest.mark.parametrize(
    "invalid_external",
    [
        "^tests@21 tests",
    ],
)
def test_parse_invalid_external_definition(invalid_external):
    with pytest.raises(ValueError, match="Failed to parse external definition"):
        utils.parse_external_definition(
            invalid_external, "/trunk/externals", "http://svn.example.org/repo"
        )


FIRST_COMMIT_DATE = datetime(year=2020, month=7, day=14, tzinfo=timezone.utc)
SECOND_COMMIT_DATE = FIRST_COMMIT_DATE + timedelta(minutes=10)
THIRD_COMMIT_DATE = SECOND_COMMIT_DATE + timedelta(hours=1)


@pytest.fixture
def repo_url(repo_url):
    add_commit(
        repo_url,
        "Add trunk/foo/foo path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/foo/foo",
                data=b"foo",
            )
        ],
        FIRST_COMMIT_DATE,
    )
    add_commit(
        repo_url,
        "Add trunk/bar/bar path",
        [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path="trunk/bar/bar",
                data=b"bar",
            )
        ],
        SECOND_COMMIT_DATE,
    )
    add_commit(
        repo_url,
        "Remove trunk/foo/foo path",
        [
            CommitChange(
                change_type=CommitChangeType.Delete,
                path="trunk/foo/",
            )
        ],
        THIRD_COMMIT_DATE,
    )
    return repo_url


def test_get_repo_root_url(repo_url):
    utils.get_repo_root_url(repo_url) == repo_url
    utils.get_repo_root_url(f"{repo_url}/trunk/foo/foo") == repo_url
    utils.get_repo_root_url(f"{repo_url}/trunk/bar/bar") == repo_url


def test_get_head_revision_at_date(repo_url):
    utils.get_head_revision_at_date(repo_url, FIRST_COMMIT_DATE) == 1
    utils.get_head_revision_at_date(repo_url, SECOND_COMMIT_DATE) == 2
    utils.get_head_revision_at_date(repo_url, THIRD_COMMIT_DATE) == 3

    utils.get_head_revision_at_date(
        repo_url, FIRST_COMMIT_DATE + (SECOND_COMMIT_DATE - FIRST_COMMIT_DATE) / 2
    ) == 1

    utils.get_head_revision_at_date(
        repo_url, SECOND_COMMIT_DATE + (THIRD_COMMIT_DATE - SECOND_COMMIT_DATE) / 2
    ) == 2
