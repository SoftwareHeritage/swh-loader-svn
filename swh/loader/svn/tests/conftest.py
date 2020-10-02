# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pytest


@pytest.fixture
def swh_loader_config(swh_storage_backend_config) -> Dict[str, Any]:
    swh_storage_backend_config["journal_writer"] = {}
    return {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "filter"},
                {
                    "cls": "buffer",
                    "min_batch_size": {
                        "content": 10000,
                        "content_bytes": 1073741824,
                        "directory": 2500,
                        "revision": 10,
                        "release": 100,
                    },
                },
                swh_storage_backend_config,
            ],
        },
        "check_revision": {"limit": 100, "status": False},
        "log_db": "dbname=softwareheritage-log",
        "temp_directory": "/tmp",
    }
