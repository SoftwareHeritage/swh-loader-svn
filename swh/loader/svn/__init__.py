# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict


def register() -> Dict[str, Any]:
    from swh.loader.svn.loader import SvnLoaderFromRemoteDump

    return {
        "task_modules": [f"{__name__}.tasks"],
        "loader": SvnLoaderFromRemoteDump,
    }


def register_export() -> Dict[str, Any]:
    from swh.loader.svn.directory import SvnExportLoader

    return {
        "task_modules": [],
        "loader": SvnExportLoader,
    }


def register_no_dump() -> Dict[str, Any]:
    from swh.loader.svn.loader import SvnLoader

    return {
        "task_modules": [f"{__name__}.tasks"],
        "loader": SvnLoader,
    }


def register_from_dump() -> Dict[str, Any]:
    from swh.loader.svn.loader import SvnLoaderFromDump

    return {
        "task_modules": [f"{__name__}.tasks"],
        "loader": SvnLoaderFromDump,
    }
