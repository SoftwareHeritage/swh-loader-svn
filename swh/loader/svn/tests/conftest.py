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
            'cls': 'pipeline',
            'steps': [
                {
                    'cls': 'validate'
                },
                {
                    'cls': 'filter'
                },
                {
                    'cls': 'buffer',
                    'min_batch_size': {
                        'content': 10000,
                        'content_bytes': 1073741824,
                        'directory': 2500,
                        'revision': 10,
                        'release': 100,
                    },
                },
                {
                    'cls': 'memory'
                },
            ]
        },
        'check_revision': {'limit': 100, 'status': False},
        'debug': False,
        'log_db': 'dbname=softwareheritage-log',
        'save_data': False,
        'save_data_path': '',
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
