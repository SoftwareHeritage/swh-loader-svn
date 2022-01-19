# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import pty
import shutil
from subprocess import Popen

import pytest

from swh.loader.svn import utils


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


def test_init_svn_repo_from_archive_dump(datadir, tmp_path):
    """Mounting svn repository out of an archive dump is ok"""
    dump_name = "penguinsdbtools2018.dump.gz"
    dump_path = os.path.join(datadir, dump_name)

    tmp_repo, repo_path = utils.init_svn_repo_from_archive_dump(
        dump_path, cleanup_dump=False, root_dir=tmp_path
    )

    assert os.path.exists(dump_path), "Dump path should still exists"
    assert os.path.exists(repo_path), "Repository should exists"


def test_init_svn_repo_from_archive_dump_and_cleanup(datadir, tmp_path):
    """Mounting svn repository out of a dump is ok"""
    dump_name = "penguinsdbtools2018.dump.gz"
    dump_ori_path = os.path.join(datadir, dump_name)

    dump_path = os.path.join(tmp_path, dump_name)
    shutil.copyfile(dump_ori_path, dump_path)

    assert os.path.exists(dump_path)
    assert os.path.exists(dump_ori_path)

    tmp_repo, repo_path = utils.init_svn_repo_from_archive_dump(
        dump_path, root_dir=tmp_path
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
        ("https://svn.example.org/foo", ["../bar"], "https://svn.example.org/bar",),
    ],
)
def test_svn_urljoin(base_url, paths_to_join, expected_result):
    assert utils.svn_urljoin(base_url, *paths_to_join) == expected_result


@pytest.mark.parametrize(
    "external, dir_path, repo_url, expected_result",
    [
        # subversion < 1.5
        (
            "third-party/sounds             http://svn.example.com/repos/sounds",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/sounds", "http://svn.example.com/repos/sounds", None, False),
        ),
        (
            "third-party/skins -r148        http://svn.example.com/skinproj",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/skins", "http://svn.example.com/skinproj", 148, False),
        ),
        (
            "third-party/skins/toolkit -r21 http://svn.example.com/skin-maker",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                "third-party/skins/toolkit",
                "http://svn.example.com/skin-maker",
                21,
                False,
            ),
        ),
        # subversion >= 1.5
        (
            "      http://svn.example.com/repos/sounds third-party/sounds",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/sounds", "http://svn.example.com/repos/sounds", None, False),
        ),
        (
            "-r148 http://svn.example.com/skinproj third-party/skins",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/skins", "http://svn.example.com/skinproj", 148, False),
        ),
        (
            "-r 21 http://svn.example.com/skin-maker third-party/skins/toolkit",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                "third-party/skins/toolkit",
                "http://svn.example.com/skin-maker",
                21,
                False,
            ),
        ),
        (
            "http://svn.example.com/repos/sounds third-party/sounds",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/sounds", "http://svn.example.com/repos/sounds", None, False),
        ),
        (
            "http://svn.example.com/skinproj@148 third-party/skins",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/skins", "http://svn.example.com/skinproj", 148, False),
        ),
        (
            "http://anon:anon@svn.example.com/skin-maker@21 third-party/skins/toolkit",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                "third-party/skins/toolkit",
                "http://anon:anon@svn.example.com/skin-maker",
                21,
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
                False,
            ),
        ),
        (
            "/skinproj@148 third-party/skins",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("third-party/skins", "http://svn.example.org/skinproj", 148, True),
        ),
        (
            "//svn.example.com/skin-maker@21 third-party/skins/toolkit",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                "third-party/skins/toolkit",
                "http://svn.example.com/skin-maker",
                21,
                True,
            ),
        ),
        (
            "../skins skins",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("skins", "http://svn.example.org/repos/test/trunk/skins", None, False),
        ),
        (
            "../skins skins",
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("skins", "http://svn.example.org/repos/test/trunk/skins", None, False),
        ),
        # subversion >= 1.6
        (
            'http://svn.thirdparty.com/repos/My%20Project "My Project"',
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("My Project", "http://svn.thirdparty.com/repos/My%20Project", None, False),
        ),
        (
            'http://svn.thirdparty.com/repos/My%20%20%20Project "My   Project"',
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                "My   Project",
                "http://svn.thirdparty.com/repos/My%20%20%20Project",
                None,
                False,
            ),
        ),
        (
            'http://svn.thirdparty.com/repos/%22Quotes%20Too%22 \\"Quotes\\ Too\\"',
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                '"Quotes Too"',
                "http://svn.thirdparty.com/repos/%22Quotes%20Too%22",
                None,
                False,
            ),
        ),
        (
            'http://svn.thirdparty.com/repos/%22Quotes%20%20%20Too%22 \\"Quotes\\ \\ \\ Too\\"',  # noqa
            "trunk/externals",
            "http://svn.example.org/repos/test",
            (
                '"Quotes   Too"',
                "http://svn.thirdparty.com/repos/%22Quotes%20%20%20Too%22",
                None,
                False,
            ),
        ),
        # edge cases
        (
            '-r1 http://svn.thirdparty.com/repos/test "trunk/PluginFramework"',
            "trunk/externals",
            "http://svn.example.org/repos/test",
            ("trunk/PluginFramework", "http://svn.thirdparty.com/repos/test", 1, False),
        ),
        (
            "external -r 9 http://svn.thirdparty.com/repos/test",
            "tags",
            "http://svn.example.org/repos/test",
            ("external", "http://svn.thirdparty.com/repos/test", 9, False),
        ),
        (
            "./external http://svn.thirdparty.com/repos/test",
            "tags",
            "http://svn.example.org/repos/test",
            ("external", "http://svn.thirdparty.com/repos/test", None, False),
        ),
        (
            "external ttp://svn.thirdparty.com/repos/test",
            "tags",
            "http://svn.example.org/repos/test",
            ("external", "ttp://svn.thirdparty.com/repos/test", None, False),
        ),
        (
            "C:\\code\\repo\\external http://svn.thirdparty.com/repos/test",
            "tags",
            "http://svn.example.org/repos/test",
            ("C:coderepoexternal", "http://svn.thirdparty.com/repos/test", None, False),
        ),
        (
            "C:\\\\code\\\\repo\\\\external http://svn.thirdparty.com/repos/test",
            "tags",
            "http://svn.example.org/repos/test",
            (
                "C:\\code\\repo\\external",
                "http://svn.thirdparty.com/repos/test",
                None,
                False,
            ),
        ),
    ],
)
def test_parse_external_definition(external, dir_path, repo_url, expected_result):
    assert (
        utils.parse_external_definition(external, dir_path, repo_url) == expected_result
    )
