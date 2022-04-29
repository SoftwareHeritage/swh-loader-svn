# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import pytest

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.utils import create_origin_task_dict


@pytest.fixture(autouse=True)
def celery_worker_and_swh_config(swh_scheduler_celery_worker, swh_config):
    pass


@pytest.fixture
def svn_lister():
    return Lister(name="svn-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def svn_listed_origin(svn_lister):
    return ListedOrigin(
        lister_id=svn_lister.id, url="svn://example.org/repo", visit_type="svn"
    )


@pytest.fixture
def task_dict(svn_lister, svn_listed_origin):
    return create_origin_task_dict(svn_listed_origin, svn_lister)


def test_svn_loader(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.LoadSvnRepository",
        kwargs=dict(
            url="some-technical-url", origin_url="origin-url", visit_date="now"
        ),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    task_dict,
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.LoadSvnRepository",
        args=task_dict["arguments"]["args"],
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_from_dump(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoaderFromDumpArchive.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.MountAndLoadSvnRepository",
        kwargs=dict(url="some-url", archive_path="some-path", visit_date="now"),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_from_dump_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    svn_lister,
    svn_listed_origin,
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoaderFromDumpArchive.load")
    mock_loader.return_value = {"status": "eventful"}

    svn_listed_origin.extra_loader_arguments = {"archive_path": "some-path"}

    task_dict = create_origin_task_dict(svn_listed_origin, svn_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.MountAndLoadSvnRepository",
        args=task_dict["arguments"]["args"],
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_from_remote_dump(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoaderFromRemoteDump.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.DumpMountAndLoadSvnRepository",
        kwargs=dict(
            url="some-remote-dump-url", origin_url="origin-url", visit_date="now"
        ),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_svn_loader_from_remote_dump_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    task_dict,
):
    mock_loader = mocker.patch("swh.loader.svn.loader.SvnLoaderFromRemoteDump.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.svn.tasks.DumpMountAndLoadSvnRepository",
        args=task_dict["arguments"]["args"],
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
