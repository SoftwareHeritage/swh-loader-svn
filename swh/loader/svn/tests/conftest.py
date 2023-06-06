# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import subprocess
from typing import Any, Dict
import uuid

import pytest

from swh.loader.svn.loader import SvnLoader, SvnLoaderFromRemoteDump
from swh.loader.svn.svn_repo import SvnRepo
from swh.scheduler.model import Lister

from .utils import create_repo

NAMESPACE = "swh.loader.svn"


@pytest.fixture(params=[SvnLoader, SvnLoaderFromRemoteDump])
def svn_loader_cls(request):
    return request.param


@pytest.fixture
def swh_storage_backend_config(swh_storage_backend_config):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    return {
        "cls": "filter",
        "storage": {
            "cls": "buffer",
            "min_batch_size": {
                "content": 10000,
                "content_bytes": 1073741824,
                "directory": 2500,
                "revision": 10,
                "release": 100,
            },
            "storage": swh_storage_backend_config,
        },
    }


@pytest.fixture
def swh_loader_config(swh_storage_backend_config) -> Dict[str, Any]:
    return {
        "storage": swh_storage_backend_config,
        "check_revision": 100,
        "temp_directory": "/tmp",
    }


@pytest.fixture
def repo_url(tmpdir_factory):
    # create a repository
    return create_repo(tmpdir_factory.mktemp("repos"))


@pytest.fixture(autouse=True)
def svn_retry_sleep_mocker(mocker):
    mocker.patch.object(SvnRepo.export.retry, "sleep")
    mocker.patch.object(SvnRepo.checkout.retry, "sleep")
    mocker.patch.object(SvnRepo.propget.retry, "sleep")
    mocker.patch.object(SvnRepo.remote_access.retry, "sleep")
    mocker.patch.object(SvnRepo.info.retry, "sleep")
    mocker.patch.object(SvnRepo.commit_info.retry, "sleep")


@pytest.fixture
def svnserve():
    """Fixture wrapping svnserve execution and ensuring to terminate it
    after test run"""
    svnserve_proc = None

    def run_svnserve(repo_root, port):
        nonlocal svnserve_proc
        svnserve_proc = subprocess.Popen(
            [
                "svnserve",
                "-d",
                "--foreground",
                "--listen-port",
                str(port),
                "--root",
                repo_root,
            ]
        )

    yield run_svnserve

    svnserve_proc.terminate()


@pytest.fixture
def svn_lister():
    return Lister(name="svn-lister", instance_name="example", id=uuid.uuid4())
