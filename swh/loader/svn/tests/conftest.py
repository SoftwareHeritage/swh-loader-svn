# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import yaml

from typing import Any, Dict

from swh.scheduler.tests.conftest import swh_app  # noqa


@pytest.fixture
def swh_loader_config() -> Dict[str, Any]:
    return {
        'storage': {
            'cls': 'memory',
        },
        'check_revision': {'limit': 100, 'status': False},
        'content_packet_block_size_bytes': 104857600,
        'content_packet_size': 10000,
        'content_packet_size_bytes': 1073741824,
        'content_size_limit': 104857600,
        'debug': False,
        'directory_packet_size': 2500,
        'log_db': 'dbname=softwareheritage-log',
        'occurrence_packet_size': 1000,
        'release_packet_size': 1000,
        'revision_packet_size': 10,
        'save_data': False,
        'save_data_path': '',
        'send_contents': True,
        'send_directories': True,
        'send_occurrences': True,
        'send_releases': True,
        'send_revisions': True,
        'send_snapshot': True,
        'temp_directory': '/tmp',
    }


@pytest.fixture
def swh_config(swh_loader_config, monkeypatch, tmp_path):
    conffile = os.path.join(str(tmp_path), 'loader.yml')
    with open(conffile, 'w') as f:
        f.write(yaml.dump(swh_loader_config))
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


@pytest.fixture(scope='session')
def celery_includes():
    return [
        'swh.loader.svn.tasks',
    ]
