# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.loader.svn.fast_crawler import crawl_repository

from .utils import CommitChange, CommitChangeType, add_commit


def test_crawl_repository_bad_arg_types():
    with pytest.raises(TypeError):
        crawl_repository(repo_url=1)

    with pytest.raises(TypeError):
        crawl_repository("https://svn.example.org", revnum="1")


def test_crawl_repository_runtime_error():
    invalid_repo_url = "file:///tmp/svn/repo/not/found"
    with pytest.raises(
        RuntimeError,
        match=f"Unable to connect to a repository at URL '{invalid_repo_url}'",
    ):
        crawl_repository(invalid_repo_url)


def test_crawl_repository(repo_url):
    commits = [
        {
            "message": "Initial commit",
            "changes": [
                CommitChange(
                    change_type=CommitChangeType.AddOrUpdate,
                    path="code/hello/hello-world",
                    properties={"svn:executable": "*"},
                    data=b"#!/bin/bash\necho Hello World !",
                )
            ],
        },
        {
            "message": "Second commit",
            "changes": [
                CommitChange(
                    change_type=CommitChangeType.AddOrUpdate,
                    path="code/externals/",
                    properties={
                        "svn:externals": "https://svn.example.org/project project"
                    },
                )
            ],
        },
    ]

    for i, commit in enumerate(commits):
        add_commit(
            repo_url,
            commit["message"],
            commit["changes"],
        )

        paths = crawl_repository(repo_url, revnum=i + 1)
        for change in commit["changes"]:
            path = change["path"].rstrip("/")
            path_split = path.split("/")
            for i in range(len(path_split)):
                assert "/".join(path_split[: i + 1]) in paths
            for prop, value in change["properties"].items():
                assert paths[path]["props"][prop] == value

    assert crawl_repository(repo_url, revnum=len(commits)) == crawl_repository(repo_url)
