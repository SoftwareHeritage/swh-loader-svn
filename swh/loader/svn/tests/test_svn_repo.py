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
FILENAMES = ("foo", "bar", "baz")
COMMITS = [
    {
        "message": f"Create trunk/{file} file and tags/1.0/{file}",
        "date": FIRST_COMMIT_DATE + i * timedelta(days=NB_DAYS_BETWEEN_COMMITS),
        "changes": [
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path=f"trunk/{file}",
                data=file.encode(),
            ),
            CommitChange(
                change_type=CommitChangeType.AddOrUpdate,
                path=f"tags/1.0/{file}",
                data=file.encode(),
            ),
        ],
    }
    for i, file in enumerate(FILENAMES)
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


@pytest.fixture
def svn_repo_first_tag(repo_url):
    return SvnRepo(repo_url + "/tags/1.0")


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


def test_svn_repo_export_temporary_subproject(svn_repo_first_tag, mocker):
    svn_repo_export = mocker.spy(svn_repo_first_tag, "export")
    # export tags/1.0/ directory of the repository at HEAD revision
    _, local_url = svn_repo_first_tag.export_temporary(len(COMMITS))
    # check first tag URL was used as export URL
    assert svn_repo_export.call_args_list[0][0][0].endswith("tags/1.0")
    # get exported filesystem
    export_content = list(os.walk(local_url))
    # should be a single directory containing only files
    assert len(export_content) == 1
    _, subdirs, files = export_content[0]
    assert len(subdirs) == 0
    assert len(files) == len(FILENAMES)
    # check that paths outside the export path were not exported
    trunk_path = os.path.join(local_url, b"../../trunk")
    assert not os.path.exists(trunk_path)
