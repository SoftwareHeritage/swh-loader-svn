# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pytest

from swh.scheduler.model import ListedOrigin

from .conftest import NAMESPACE


@pytest.fixture
def swh_loader_config(swh_storage_backend_config) -> Dict[str, Any]:
    return {
        "storage": swh_storage_backend_config,
    }


@pytest.fixture
def svn_listed_svn_directory_origin(svn_lister):
    return ListedOrigin(
        lister_id=svn_lister.id,
        url="svn://example.org/repo",
        visit_type="directory",
    )


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [
        {"checksum_layout": "nar", "checksums": {}, "ref": "5"},
        {"checksum_layout": "standard", "checksums": {}, "ref": "6"},
    ],
)
def test_svn_directory_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    svn_lister,
    svn_listed_svn_directory_origin,
    extra_loader_arguments,
):
    svn_listed_svn_directory_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.directory.SvnExportLoader",
        task_function_name=f"{NAMESPACE}.tasks.LoadSvnExport",
        lister=svn_lister,
        listed_origin=svn_listed_svn_directory_origin,
    )
