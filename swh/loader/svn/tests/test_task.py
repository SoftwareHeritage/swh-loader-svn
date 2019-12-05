# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch


@patch('swh.loader.svn.loader.SvnLoader.load')
def test_svn_loader(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.svn.tasks.LoadSvnRepository',
        (), dict(url='some-technical-url', origin_url='origin-url'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}


@patch('swh.loader.svn.loader.SvnLoaderFromDumpArchive.load')
def test_svn_loader_from_dump(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.svn.tasks.MountAndLoadSvnRepository',
        (), dict(url='some-url', archive_path='some-path'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}


@patch('swh.loader.svn.loader.SvnLoaderFromRemoteDump.load')
def test_svn_loader_from_remote_dump(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.svn.tasks.DumpMountAndLoadSvnRepository',
        (), dict(url='some-remote-dump-url', origin_url='origin-url'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}
