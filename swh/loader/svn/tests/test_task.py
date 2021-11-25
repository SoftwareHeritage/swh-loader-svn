# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone

import pytest

from swh.loader.svn.tasks import convert_to_datetime


def test_svn_loader(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.LoadSvnRepository",
        kwargs=dict(url="some-technical-url", origin_url="origin-url"),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_from_dump(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoaderFromDumpArchive.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.MountAndLoadSvnRepository",
        kwargs=dict(url="some-url", archive_path="some-path"),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_from_remote_dump(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoaderFromRemoteDump.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.DumpMountAndLoadSvnRepository",
        kwargs=dict(url="some-remote-dump-url", origin_url="origin-url"),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


@pytest.mark.parametrize(
    "date,expected_result",
    [
        (None, None),
        (
            "2021-11-23 09:41:02.434195+00:00",
            datetime(2021, 11, 23, 9, 41, 2, 434195, tzinfo=timezone.utc),
        ),
        ("23112021", None,),  # failure to parse
    ],
)
def test_convert_to_datetime(date, expected_result):
    assert convert_to_datetime(date) == expected_result
