# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.model import ListedOrigin

from .conftest import NAMESPACE


@pytest.fixture
def svn_listed_origin(svn_lister):
    return ListedOrigin(
        lister_id=svn_lister.id, url="svn://example.org/repo", visit_type="svn"
    )


@pytest.mark.parametrize("extra_loader_arguments", [{}, {"visit_date": "now"}])
def test_svn_loader_from_remote_dump_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    svn_lister,
    svn_listed_origin,
    extra_loader_arguments,
):
    svn_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.loader.SvnLoaderFromRemoteDump",
        task_function_name=f"{NAMESPACE}.tasks.DumpMountAndLoadSvnRepository",
        lister=svn_lister,
        listed_origin=svn_listed_origin,
    )


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [{"dump_path": "some-path"}, {"dump_path": "some-path", "visit_date": "now"}],
)
def test_svn_loader_from_dump_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    svn_lister,
    svn_listed_origin,
    extra_loader_arguments,
):
    svn_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.loader.SvnLoaderFromDump",
        task_function_name=f"{NAMESPACE}.tasks.MountAndLoadSvnRepository",
        lister=svn_lister,
        listed_origin=svn_listed_origin,
    )
