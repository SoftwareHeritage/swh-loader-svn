# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import closing
import socket
import subprocess
import time
from typing import Any, Dict
import uuid

import pytest

from swh.loader.svn.loader import SvnLoader, SvnLoaderFromRemoteDump
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
def mock_sleep(mocker):
    return mocker.patch("time.sleep")


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# https://gist.github.com/butla/2d9a4c0f35ea47b7452156c96a4e7b12
def wait_for_port(port: int, host: str = "localhost", timeout: float = 5.0):
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(0.01)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError(
                    f"Waited too long for the port {port} on host {host} "
                    "to start accepting connections."
                ) from ex


@pytest.fixture
def svnserve():
    """Fixture wrapping svnserve execution and ensuring to terminate it
    after test run"""
    svnserve_proc = None

    def run_svnserve(repo_root):
        nonlocal svnserve_proc
        port = find_free_port()
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
        wait_for_port(port)
        return port

    yield run_svnserve

    svnserve_proc.terminate()


@pytest.fixture
def svn_lister():
    return Lister(name="svn-lister", instance_name="example", id=uuid.uuid4())
