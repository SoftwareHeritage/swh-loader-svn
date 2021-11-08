# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict


def register() -> Dict[str, Any]:
    from swh.loader.svn.loader import SvnLoaderFromRemoteDump

    return {
        "task_modules": ["%s.tasks" % __name__],
        "loader": SvnLoaderFromRemoteDump,
    }
