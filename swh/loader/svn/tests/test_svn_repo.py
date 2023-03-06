# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import gc
import os

import pytest

from swh.loader.svn.svn_repo import SvnRepo

from .utils import CommitChange, CommitChangeType, add_commit

FIRST_COMMIT_DATE = datetime(year=2019, month=1, day=1, tzinfo=timezone.utc)
NB_DAYS_BETWEEN_COMMITS = 2
COMMITS = [
    {
        "message": f"Create trunk/{file} file",
        "date": FIRST_COMMIT_DATE + i * timedelta(days=NB_DAYS_BETWEEN_COMMITS),
        "changes": [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path=f"trunk/{file}",
                data=file.encode(),
            ),
        ],
    }
    for i, file in enumerate(("foo", "bar", "baz"))
]


@pytest.fixture
def repo_url(repo_url):
    for commit in COMMITS:
        add_commit(
            repo_url,
            commit["message"],
            commit["changes"],
            commit["date"],
        )
    return repo_url


def test_svn_repo_temp_dir_cleanup(repo_url):
    svn_repo = SvnRepo(repo_url)
    tmp_dir = svn_repo.local_dirname
    assert os.path.exists(tmp_dir)
    del svn_repo
    gc.collect()
    assert not os.path.exists(tmp_dir)


@pytest.fixture
def svn_repo(repo_url):
    return SvnRepo(repo_url)


def test_svn_repo_head_revision(svn_repo):
    assert svn_repo.head_revision() == len(COMMITS)


def _assert_commit(i, commit):
    assert commit["rev"] == i + 1
    assert commit["message"] == COMMITS[i]["message"].encode()
    assert commit["has_changes"]
    assert commit["changed_paths"]
    assert commit["author_date"].to_datetime() == COMMITS[i]["date"]


def test_svn_repo_logs(svn_repo):
    for i, commit in enumerate(svn_repo.logs(1, len(COMMITS))):
        _assert_commit(i, commit)


def test_svn_repo_commit_info(svn_repo):
    for i in range(len(COMMITS)):
        commit = svn_repo.commit_info(i + 1)
        _assert_commit(i, commit)


def test_svn_repo_info(svn_repo):
    info = svn_repo.info()
    assert info.url == svn_repo.origin_url
    assert info.repos_root_url == svn_repo.origin_url
    assert info.revision == len(COMMITS)


def test_svn_repo_get_head_revision_at_date(svn_repo):
    for i in range(len(COMMITS)):
        assert svn_repo.get_head_revision_at_date(COMMITS[i]["date"]) == i + 1
        if i == 0:
            with pytest.raises(
                ValueError, match="First revision date is greater than reference date"
            ):
                svn_repo.get_head_revision_at_date(
                    COMMITS[i]["date"] - timedelta(days=NB_DAYS_BETWEEN_COMMITS - 1)
                )
        else:
            assert (
                svn_repo.get_head_revision_at_date(
                    COMMITS[i]["date"] - timedelta(days=NB_DAYS_BETWEEN_COMMITS - 1)
                )
                == i
            )
            assert (
                svn_repo.get_head_revision_at_date(
                    COMMITS[i]["date"] + timedelta(days=NB_DAYS_BETWEEN_COMMITS - 1)
                )
                == i + 1
            )
