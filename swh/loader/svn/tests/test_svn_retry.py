# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import pytest
from subvertpy import SubversionException
from subvertpy.ra import Auth, RemoteAccess, get_username_provider

from swh.loader.svn.svn import SvnRepo
from swh.loader.svn.svn_retry import SVN_RETRY_MAX_ATTEMPTS, SVN_RETRY_WAIT_EXP_BASE
from swh.loader.tests import prepare_repository_from_archive


def _get_repo_url(archive_name, datadir, tmp_path):
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    return prepare_repository_from_archive(archive_path, "pkg-gourmet", tmp_path)


@pytest.fixture()
def sample_repo_url(datadir, tmp_path):
    return _get_repo_url("pkg-gourmet", datadir, tmp_path)


@pytest.fixture()
def sample_repo_with_externals_url(datadir, tmp_path):
    return _get_repo_url("pkg-gourmet-with-external-id", datadir, tmp_path)


class SVNClientWrapper:
    """Methods of subvertpy.client.Client cannot be patched by mocker fixture
    as they are read only attributes due to subvertpy.client module being
    a C extension module. So we use that wrapper class instead to simulate
    mocking behavior.
    """

    def __init__(self, client, exception, nb_failed_calls):
        self.client = client
        self.exception = exception
        self.nb_failed_calls = nb_failed_calls
        self.nb_calls = 0

    def _wrapped_svn_cmd(self, svn_cmd, *args, **kwargs):
        self.nb_calls = self.nb_calls + 1
        if self.nb_calls <= self.nb_failed_calls:
            raise self.exception
        else:
            return svn_cmd(*args, **kwargs)

    def export(self, *args, **kwargs):
        return self._wrapped_svn_cmd(self.client.export, *args, **kwargs)

    def checkout(self, *args, **kwargs):
        return self._wrapped_svn_cmd(self.client.checkout, *args, **kwargs)

    def propget(self, *args, **kwargs):
        return self._wrapped_svn_cmd(self.client.propget, *args, **kwargs)

    def info(self, *args, **kwargs):
        return self._wrapped_svn_cmd(self.client.info, *args, **kwargs)


class SVNRemoteAccessWrapper:
    """Methods of subvertpy.ra.RemoteAccess cannot be patched by mocker fixture
    as they are read only attributes due to subvertpy._ra module being
    a C extension module. So we use that wrapper class instead to simulate
    mocking behavior.
    """

    def __init__(self, svn_ra, exception, nb_failed_calls):
        self.svn_ra = svn_ra
        self.exception = exception
        self.nb_failed_calls = nb_failed_calls
        self.nb_calls = 0

    def _wrapped_svn_ra_cmd(self, svn_ra_cmd, *args, **kwargs):
        self.nb_calls = self.nb_calls + 1
        if self.nb_calls <= self.nb_failed_calls:
            raise self.exception
        else:
            return svn_ra_cmd(*args, **kwargs)

    def iter_log(self, *args, **kwargs):
        return self._wrapped_svn_ra_cmd(self.svn_ra.iter_log, *args, **kwargs)


def assert_sleep_calls(mock_sleep, mocker, nb_failures):
    mock_sleep.assert_has_calls(
        [
            mocker.call(param)
            for param in [SVN_RETRY_WAIT_EXP_BASE**i for i in range(nb_failures)]
        ]
    )


RETRYABLE_EXCEPTIONS = [
    SubversionException(
        "Error running context: The server unexpectedly closed the connection.",
        120108,
    ),
    SubversionException("Connection timed out", 175012),
    SubversionException("Unable to connect to a repository at URL", 170013),
    SubversionException(
        "ra_serf: The server sent a truncated HTTP response body.", 120106
    ),
    ConnectionResetError(),
    TimeoutError(),
]


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_export_retry_success(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.export.retry, "sleep")

    nb_failed_calls = 2
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    export_path = os.path.join(tmp_path, "export")
    svnrepo.export(sample_repo_url, export_path)
    assert os.path.exists(export_path)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_export_retry_failure(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.export.retry, "sleep")

    nb_failed_calls = SVN_RETRY_MAX_ATTEMPTS
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    with pytest.raises(type(exception_to_retry)):
        export_path = os.path.join(tmp_path, "export")
        svnrepo.export(sample_repo_url, export_path)

    assert not os.path.exists(export_path)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls - 1)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_checkout_retry_success(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.checkout.retry, "sleep")

    nb_failed_calls = 2
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    checkout_path = os.path.join(tmp_path, "checkout")
    svnrepo.checkout(sample_repo_url, checkout_path, svnrepo.head_revision())
    assert os.path.exists(checkout_path)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_checkout_retry_failure(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.checkout.retry, "sleep")

    nb_failed_calls = SVN_RETRY_MAX_ATTEMPTS
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    checkout_path = os.path.join(tmp_path, "checkout")
    with pytest.raises(type(exception_to_retry)):
        svnrepo.checkout(sample_repo_url, checkout_path, svnrepo.head_revision())

    assert not os.path.exists(checkout_path)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls - 1)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_propget_retry_success(
    mocker, tmp_path, sample_repo_with_externals_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_with_externals_url,
        sample_repo_with_externals_url,
        tmp_path,
        max_content_length=100000,
    )

    checkout_path = os.path.join(tmp_path, "checkout")
    svnrepo.checkout(
        sample_repo_with_externals_url,
        checkout_path,
        svnrepo.head_revision(),
        ignore_externals=True,
    )

    mock_sleep = mocker.patch.object(svnrepo.propget.retry, "sleep")

    nb_failed_calls = 2
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    externals = svnrepo.propget("svn:externals", checkout_path, None, None, True)

    assert externals

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_propget_retry_failure(
    mocker, tmp_path, sample_repo_with_externals_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_with_externals_url,
        sample_repo_with_externals_url,
        tmp_path,
        max_content_length=100000,
    )

    checkout_path = os.path.join(tmp_path, "checkout")
    svnrepo.checkout(
        sample_repo_with_externals_url,
        checkout_path,
        svnrepo.head_revision(),
        ignore_externals=True,
    )

    mock_sleep = mocker.patch.object(svnrepo.propget.retry, "sleep")

    nb_failed_calls = SVN_RETRY_MAX_ATTEMPTS
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    with pytest.raises(type(exception_to_retry)):
        svnrepo.propget("svn:externals", checkout_path, None, None, True)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls - 1)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_remote_access_retry_success(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):

    nb_failed_calls = 2
    mock_ra = mocker.patch("swh.loader.svn.svn.RemoteAccess")
    remote_access = RemoteAccess(sample_repo_url, auth=Auth([get_username_provider()]))
    mock_ra.side_effect = (
        [exception_to_retry] * nb_failed_calls
        + [remote_access]
        + [exception_to_retry] * nb_failed_calls
        + [remote_access]
    )

    mock_sleep = mocker.patch.object(SvnRepo.remote_access.retry, "sleep")

    SvnRepo(
        sample_repo_url,
        sample_repo_url,
        tmp_path,
        max_content_length=100000,
    )

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_remote_access_retry_failure(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):

    nb_failed_calls = SVN_RETRY_MAX_ATTEMPTS
    mock_ra = mocker.patch("swh.loader.svn.svn.RemoteAccess")
    remote_access = RemoteAccess(sample_repo_url, auth=Auth([get_username_provider()]))
    mock_ra.side_effect = (
        [exception_to_retry] * nb_failed_calls
        + [remote_access]
        + [exception_to_retry] * nb_failed_calls
        + [remote_access]
    )

    mock_sleep = mocker.patch.object(SvnRepo.remote_access.retry, "sleep")

    with pytest.raises(type(exception_to_retry)):
        SvnRepo(
            sample_repo_url,
            sample_repo_url,
            tmp_path,
            max_content_length=100000,
        )

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls - 1)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_info_retry_success(mocker, tmp_path, sample_repo_url, exception_to_retry):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.info.retry, "sleep")

    nb_failed_calls = 2
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    info = svnrepo.info(sample_repo_url)
    assert info

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_info_retry_failure(mocker, tmp_path, sample_repo_url, exception_to_retry):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.info.retry, "sleep")

    nb_failed_calls = SVN_RETRY_MAX_ATTEMPTS
    svnrepo.client = SVNClientWrapper(
        svnrepo.client, exception_to_retry, nb_failed_calls
    )

    with pytest.raises(type(exception_to_retry)):
        svnrepo.info(sample_repo_url)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls - 1)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_commit_info_retry_success(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.commit_info.retry, "sleep")

    nb_failed_calls = 2
    svnrepo.conn_log = SVNRemoteAccessWrapper(
        svnrepo.conn_log, exception_to_retry, nb_failed_calls
    )

    commit = svnrepo.commit_info(revision=1)
    assert commit

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls)


@pytest.mark.parametrize("exception_to_retry", RETRYABLE_EXCEPTIONS)
def test_svn_commit_info_retry_failure(
    mocker, tmp_path, sample_repo_url, exception_to_retry
):
    svnrepo = SvnRepo(
        sample_repo_url, sample_repo_url, tmp_path, max_content_length=100000
    )

    mock_sleep = mocker.patch.object(svnrepo.commit_info.retry, "sleep")

    nb_failed_calls = SVN_RETRY_MAX_ATTEMPTS
    svnrepo.conn_log = SVNRemoteAccessWrapper(
        svnrepo.conn_log, exception_to_retry, nb_failed_calls
    )

    with pytest.raises(type(exception_to_retry)):
        svnrepo.commit_info(sample_repo_url)

    assert_sleep_calls(mock_sleep, mocker, nb_failed_calls - 1)
