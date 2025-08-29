# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""SVN client in charge of iterating over svn logs and yield commit
representations including the hash tree/content computations per svn
commit.

"""

import bisect
from contextlib import contextmanager
from datetime import datetime
import logging
import os
import shutil
import tempfile
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from subvertpy import SubversionException, client, properties, wc
from subvertpy.ra import (
    Auth,
    RemoteAccess,
    get_simple_prompt_provider,
    get_ssl_client_cert_file_provider,
    get_ssl_client_cert_pw_file_provider,
    get_ssl_server_trust_file_provider,
    get_username_provider,
)

from swh.loader.exception import NotFound
from swh.model.from_disk import Directory as DirectoryFromDisk
from swh.model.model import Content, Directory, SkippedContent

from . import converters, fast_crawler, replay
from .svn_retry import svn_retry
from .utils import is_recursive_external, parse_external_definition, quote_svn_url

# When log message contains empty data
DEFAULT_AUTHOR_MESSAGE = b""

logger = logging.getLogger(__name__)


@contextmanager
def ssh_askpass_anonymous():
    """Context manager to prevent blocking subversion checkout/export operation
    due to password prompt triggered by an external definition whose target URL
    starts with 'svn+ssh://<user>@'. The requested password is automatically set
    to 'anonymous' in that case."""
    with tempfile.NamedTemporaryFile(mode="w") as askpass_script:
        askpass_script.write("#!/bin/sh\necho anonymous")
        askpass_script.flush()
        os.chmod(askpass_script.name, 0o700)
        os.environ["SSH_ASKPASS_REQUIRE"] = "force"
        os.environ["SSH_ASKPASS"] = askpass_script.name
        yield askpass_script


class SvnRepo:
    """Svn repository representation.

    Args:
        remote_url: Remove svn repository url
        origin_url: Associated origin identifier
        local_dirname: Path to write intermediary svn action results

    """

    def __init__(
        self,
        remote_url: str,
        origin_url: Optional[str] = None,
        local_dirname: Optional[str] = None,
        max_content_length: int = 100000,
        debug: bool = False,
        username: str = "",
        password: str = "",
        revision: Optional[int] = None,
    ):
        if origin_url is None:
            origin_url = remote_url

        self.manage_directory = False
        if local_dirname is None:
            local_dirname = tempfile.mkdtemp()
            self.manage_directory = True
        self.local_dirname = local_dirname

        self.origin_url = origin_url

        # default auth provider for anonymous access
        auth_providers = [get_username_provider()]

        # check if basic auth is required
        parsed_origin_url = urlparse(origin_url)
        self.username = parsed_origin_url.username or username
        self.password = parsed_origin_url.password or password

        if self.username:
            # add basic auth provider for username/password
            auth_providers.append(
                get_simple_prompt_provider(
                    lambda realm, uname, may_save: (
                        self.username,
                        self.password,
                        False,
                    ),
                    0,
                )
            )

            if "@" in origin_url:
                # we need to remove the authentication part in the origin URL to avoid
                # errors when calling subversion API through subvertpy
                self.origin_url = urlunparse(
                    parsed_origin_url._replace(
                        netloc=parsed_origin_url.netloc.split("@", 1)[1]
                    )
                )
                if origin_url == remote_url:
                    remote_url = self.origin_url

        self.remote_url = remote_url.rstrip("/")

        auth_providers += [
            get_ssl_client_cert_file_provider(),
            get_ssl_client_cert_pw_file_provider(),
            get_ssl_server_trust_file_provider(),
        ]

        self.auth = Auth(auth_providers)
        # one client for update operation
        self.client = client.Client(auth=self.auth)

        if not self.remote_url.startswith("file://"):
            # use redirection URL if any for remote operations
            self.remote_url = self.info(
                self.remote_url, revision=revision, peg_revision=revision
            ).url

        self.remote_access_url = self.remote_url

        local_name = os.path.basename(self.remote_url)
        self.local_url = os.path.join(self.local_dirname, local_name).encode("utf-8")

        conn = self.remote_access()
        self.uuid = conn.get_uuid().encode("utf-8")
        self.swhreplay = replay.Replay(
            conn=conn,
            rootpath=self.local_url,
            svnrepo=self,
            temp_dir=local_dirname,
            debug=debug,
            max_content_size=max_content_length,
        )
        self.max_content_length = max_content_length
        self.has_relative_externals = False
        self.has_recursive_externals = False
        self.replay_started = False

        # compute root directory path from the origin URL, required to
        # properly load the sub-tree of a repository mounted from a dump file
        repos_root_url = self.info(
            self.origin_url, revision=revision, peg_revision=revision
        ).repos_root_url
        origin_url_parsed = urlparse(self.origin_url)
        repos_root_url_parsed = urlparse(repos_root_url)
        if origin_url_parsed.scheme != repos_root_url_parsed.scheme:
            # update repos_root_url scheme in case of redirection
            repos_root_url = urlunparse(
                repos_root_url_parsed._replace(scheme=origin_url_parsed.scheme)
            )
        self.root_directory = self.origin_url.rstrip("/").replace(repos_root_url, "", 1)
        # get root repository URL from the remote URL
        self.repos_root_url = self.info(
            self.remote_url, revision=revision, peg_revision=revision
        ).repos_root_url

    def __del__(self):
        # ensure temporary directory is removed when created by constructor
        if self.manage_directory:
            self.clean_fs()

    def __str__(self):
        return str(
            {
                "swh-origin": self.origin_url,
                "remote_url": self.remote_url,
                "local_url": self.local_url,
                "uuid": self.uuid,
            }
        )

    def head_revision(self) -> int:
        """Retrieve current head revision."""
        return self.remote_access().get_latest_revnum()

    def initial_revision(self) -> int:
        """Retrieve the initial revision from which the remote url appeared."""
        return 1

    def _revision_data(self, log_entry: Tuple) -> Dict:
        changed_paths, rev, revprops, _ = log_entry

        author_date = converters.svn_date_to_swh_date(
            revprops.get(properties.PROP_REVISION_DATE)
        )

        author = converters.svn_author_to_swh_person(
            revprops.get(properties.PROP_REVISION_AUTHOR)
        )

        message = revprops.get(properties.PROP_REVISION_LOG, DEFAULT_AUTHOR_MESSAGE)

        has_changes = changed_paths is not None and any(
            (
                changed_path.startswith(self.root_directory)
                or (copyfrom_rev != -1 and self.root_directory.startswith(changed_path))
            )
            for changed_path, (_, _, copyfrom_rev, _) in changed_paths.items()
        )

        return {
            "rev": rev,
            "author_date": author_date,
            "author_name": author,
            "message": message,
            "has_changes": has_changes,
            "changed_paths": changed_paths,
        }

    def logs(
        self,
        revision_start: int,
        revision_end: int,
        paths: Optional[List[str]] = None,
        limit: int = 0,
        discover_changed_paths: bool = True,
    ) -> Iterator[Dict]:
        """Stream svn logs between revision_start and revision_end.

        Yields revision information between revision_start and revision_end.

        Args:
            revision_start: the svn revision starting bound
            revision_end: the svn revision ending bound

        Yields:
            dictionaries of revision data with the following keys:

                - rev: revision number
                - author_date: date of the commit
                - author_name: name of the author of the commit
                - message: commit message
                - has_changes: whether the commit has changes
                (can be False when loading subprojects)
                - changed_paths: list of paths changed by the commit

        """
        for log_entry in self.remote_access().iter_log(
            paths=paths,
            start=revision_start,
            end=revision_end,
            discover_changed_paths=discover_changed_paths,
            limit=limit,
        ):
            yield self._revision_data(log_entry)

    @svn_retry()
    def commit_info(self, revision: int) -> Optional[Dict]:
        """Return commit information.

        Args:
            revision: svn revision to return commit info

        Returns:
            A dictionary filled with commit info, see :meth:`swh.loader.svn.svn_repo.logs`
            for details about its content.
        """
        return next(self.logs(revision, revision), None)

    @svn_retry()
    def remote_access(self) -> RemoteAccess:
        """Simple wrapper around subvertpy.ra.RemoteAccess creation
        enabling to retry the operation if a network error occurs."""
        return RemoteAccess(self.remote_access_url, auth=self.auth)

    @svn_retry()
    def info(
        self,
        origin_url: Optional[str] = None,
        peg_revision: Optional[int] = None,
        revision: Optional[int] = None,
    ):
        """Simple wrapper around subvertpy.client.Client.info enabling to retry
        the command if a network error occurs.

        Args:
            origin_url: If provided, query info about a specific repository,
                currently set origin URL will be used otherwise
        """
        info = self.client.info(
            quote_svn_url(origin_url or self.origin_url).rstrip("/"),
            peg_revision=peg_revision,
            revision=revision,
        )
        return next(iter(info.values()))

    @svn_retry()
    def export(
        self,
        url: str,
        to: str,
        rev: Optional[int] = None,
        peg_rev: Optional[int] = None,
        recurse: bool = True,
        ignore_externals: bool = False,
        overwrite: bool = False,
        ignore_keywords: bool = False,
        remove_dest_path: bool = True,
    ) -> int:
        """Simple wrapper around subvertpy.client.Client.export enabling to retry
        the command if a network error occurs.

        See documentation of svn_client_export5 function from subversion C API
        to get details about parameters.
        """
        if remove_dest_path:
            # remove export path as command can be retried
            if os.path.isfile(to) or os.path.islink(to):
                os.remove(to)
            elif os.path.isdir(to):
                shutil.rmtree(to)
        options = []
        if rev is not None:
            options.append(f"-r {rev}")
        if recurse:
            options.append("--depth infinity")
        if ignore_externals:
            options.append("--ignore-externals")
        if overwrite:
            options.append("--force")
        if ignore_keywords:
            options.append("--ignore-keywords")
        logger.debug(
            "svn export %s %s%s %s",
            " ".join(options),
            quote_svn_url(url),
            f"@{peg_rev}" if peg_rev else "",
            to,
        )
        with ssh_askpass_anonymous():
            return self.client.export(
                quote_svn_url(url),
                to=to,
                rev=rev,
                peg_rev=peg_rev,
                recurse=recurse,
                ignore_externals=ignore_externals,
                overwrite=overwrite,
                ignore_keywords=ignore_keywords,
            )

    @svn_retry()
    def checkout(
        self,
        url: str,
        path: str,
        rev: Optional[int] = None,
        peg_rev: Optional[int] = None,
        recurse: bool = True,
        ignore_externals: bool = False,
        allow_unver_obstructions: bool = False,
    ) -> int:
        """Simple wrapper around subvertpy.client.Client.checkout enabling to retry
        the command if a network error occurs.

        See documentation of svn_client_checkout3 function from subversion C API
        to get details about parameters.
        """
        if os.path.isdir(os.path.join(path, ".svn")):
            # cleanup checkout path as command can be retried and svn working copy might
            # be locked
            wc.cleanup(path)
        elif os.path.isdir(path):
            # recursively remove checkout path otherwise if it is not a svn working copy
            shutil.rmtree(path)
        options = []
        if rev is not None:
            options.append(f"-r {rev}")
        if recurse:
            options.append("--depth infinity")
        if ignore_externals:
            options.append("--ignore-externals")
        logger.debug(
            "svn checkout %s %s%s %s",
            " ".join(options),
            quote_svn_url(url),
            f"@{peg_rev}" if peg_rev else "",
            path,
        )
        with ssh_askpass_anonymous():
            return self.client.checkout(
                quote_svn_url(url),
                path=path,
                rev=rev,
                peg_rev=peg_rev,
                recurse=recurse,
                ignore_externals=ignore_externals,
                allow_unver_obstructions=allow_unver_obstructions,
            )

    @svn_retry()
    def propget(
        self,
        name: str,
        target: str,
        peg_rev: Optional[int],
        rev: Optional[int] = None,
        recurse: bool = False,
    ) -> Dict[str, bytes]:
        """Simple wrapper around subvertpy.client.Client.propget enabling to retry
        the command if a network error occurs.

        See documentation of svn_client_propget5 function from subversion C API
        to get details about parameters.
        """
        logger.debug(
            "svn propget %s%s %s%s",
            "--recursive " if recurse else "",
            name,
            quote_svn_url(target),
            f"@{peg_rev}" if peg_rev else "",
        )
        target_is_url = urlparse(target).scheme != ""
        if target_is_url:
            # subvertpy 0.11 has a buggy implementation of propget bindings when
            # target is an URL (https://github.com/jelmer/subvertpy/issues/35)
            # as a workaround we implement propget for URL using non buggy proplist bindings
            svn_depth_infinity = 3
            svn_depth_empty = 0
            proplist = self.client.proplist(
                quote_svn_url(target),
                peg_revision=peg_rev,
                revision=rev,
                depth=svn_depth_infinity if recurse else svn_depth_empty,
            )
            return {path: props[name] for path, props in proplist if name in props}
        else:
            return self.client.propget(name, target, peg_rev, rev, recurse)

    def export_temporary(self, revision: int) -> Tuple[str, bytes]:
        """Export the repository to a given revision in a temporary location. This is up
        to the caller of this function to clean up the temporary location when done (cf.
        self.clean_fs method)

        Args:
            revision: Revision to export at

        Returns:
            The tuple local_dirname the temporary location root
            folder, local_url where the repository was exported.

        """
        local_dirname = tempfile.mkdtemp(
            dir=self.local_dirname, prefix=f"check-revision-{revision}."
        )

        local_name = os.path.basename(self.remote_url)
        local_url = os.path.join(local_dirname, local_name)

        url = self.remote_url
        # if some paths have external URLs relative to the repository URL but targeting
        # paths outside it, we need to export from the origin URL as the remote URL can
        # target a dump mounted on the local filesystem
        if self.replay_started and self.has_relative_externals:
            # externals detected while replaying revisions
            url = self.origin_url
        elif not self.replay_started:
            # revisions replay has not started, we need to check if svn:externals
            # properties are set and if some external URLs are relative to pick
            # the right export URL, recursive externals are also checked

            # recursive propget operation is terribly slow over the network,
            # so we use a much faster approach relying on a C++ extension module
            paths = fast_crawler.crawl_repository(
                self.remote_url,
                revnum=revision,
                username=self.username,
                password=self.password,
            )
            externals = {
                path: path_info["props"]["svn:externals"]
                for path, path_info in paths.items()
                if path_info["type"] == "dir" and "svn:externals" in path_info["props"]
            }

            self.has_relative_externals = False
            self.has_recursive_externals = False
            for path, external_defs in externals.items():
                if self.has_relative_externals or self.has_recursive_externals:
                    break
                for external_def in external_defs.split("\n"):
                    external_def = external_def.strip(" \t\r")
                    # skip empty line or comment
                    if not external_def or external_def.startswith("#"):
                        continue
                    external = parse_external_definition(
                        external_def.rstrip("\r"), path, self.origin_url
                    )

                    if is_recursive_external(
                        self.origin_url,
                        path,
                        external.path,
                        external.url,
                    ):
                        self.has_recursive_externals = True
                        url = self.remote_url
                        break

                    if external.relative_url:
                        self.has_relative_externals = True
                        url = self.origin_url
                        break

        try:
            url = url.rstrip("/")

            self.export(
                url,
                to=local_url,
                rev=revision,
                peg_rev=revision,
                ignore_keywords=True,
                ignore_externals=self.has_recursive_externals,
            )
        except SubversionException as se:
            if se.args[0].startswith(
                (
                    "Error parsing svn:externals property",
                    "Unrecognized format for the relative external URL",
                )
            ):
                pass
            else:
                raise

        # exported paths are relative to the repository root path so we need to
        # adjust the URL of the exported filesystem
        root_dir_local_url = os.path.join(local_url, self.root_directory.strip("/"))
        # check that root directory of a subproject did not get removed in revision
        if os.path.exists(root_dir_local_url):
            local_url = root_dir_local_url

        return local_dirname, os.fsencode(local_url)

    def swh_hash_data_per_revision(
        self, start_revision: int, end_revision: int
    ) -> Iterator[
        Tuple[
            int,
            Dict,
            Tuple[List[Content], List[SkippedContent], List[Directory]],
            DirectoryFromDisk,
        ],
    ]:
        """Compute swh hash data per each revision between start_revision and
        end_revision.

        Args:
            start_revision: starting revision
            end_revision: ending revision

        Yields:
            Tuple (rev, nextrev, commit, objects_per_path):

            - rev: current revision
            - commit: commit data (author, date, message) for such revision
            - objects_per_path: Tuple of list of objects between start_revision and
              end_revision
            - complete Directory representation

        """
        # even in incremental loading mode, we need to replay the whole set of
        # path modifications from first revision to restore possible file states induced
        # by setting svn properties on those files (end of line style for instance)
        self.replay_started = True
        first_revision = 1 if start_revision else 0  # handle empty repository edge case
        for commit in self.logs(first_revision, end_revision):
            rev = commit["rev"]
            copyfrom_revs = (
                [
                    copyfrom_rev
                    for (_, _, copyfrom_rev, _) in commit["changed_paths"].values()
                    if copyfrom_rev != -1
                ]
                if commit["changed_paths"]
                else None
            )
            low_water_mark = rev + 1
            if copyfrom_revs:
                # when files or directories in the revision to replay have been copied from
                # ancestor revisions, we need to adjust the low water mark revision used by
                # svn replay API to handle the copies in our commit editor and to ensure
                # replace operations after copy will be replayed
                low_water_mark = min(copyfrom_revs)
            objects = self.swhreplay.compute_objects(rev, low_water_mark)

            if rev >= start_revision:
                # start yielding new data to archive once we reached the revision to
                # resume the loading from
                if commit["has_changes"] or start_revision == 0:
                    # yield data only if commit has changes or if repository is empty
                    root_dir_path = self.root_directory.encode()[1:]
                    if not root_dir_path or root_dir_path in self.swhreplay.directory:
                        root_dir = self.swhreplay.directory[root_dir_path]
                    else:
                        # root directory of subproject got removed in revision, return
                        # empty directory for that edge case
                        root_dir = DirectoryFromDisk()
                    yield rev, commit, objects, root_dir

    def swh_hash_data_at_revision(
        self, revision: int
    ) -> Tuple[Dict, DirectoryFromDisk]:
        """Compute the information at a given svn revision. This is expected to be used
        for checks only.

        Yields:
            The tuple (commit dictionary, targeted directory object).

        """
        # Update disk representation of the repository at revision id
        local_dirname, local_url = self.export_temporary(revision)
        # Compute the current hashes on disk
        directory = DirectoryFromDisk.from_disk(
            path=local_url, max_content_length=self.max_content_length
        )

        # Retrieve the commit information for revision
        commit = self.commit_info(revision)

        # Clean export directory
        self.clean_fs(local_dirname)

        return commit, directory

    def clean_fs(self, local_dirname: Optional[str] = None) -> None:
        """Clean up the local working copy.

        Args:
            local_dirname: Path to remove recursively if provided. Otherwise, remove the
                temporary upper root tree used for svn repository loading.

        """
        dirname = local_dirname or self.local_dirname
        if os.path.exists(dirname):
            logger.debug("cleanup %s", dirname)
            shutil.rmtree(dirname)

    def get_head_revision_at_date(self, date: datetime) -> int:
        """Get HEAD revision number for a given date.

        Args:
            date: the reference date

        Returns:
            the revision number of the HEAD revision at that date

        Raises:
            ValueError: first revision date is greater than given date
        """

        if self.commit_info(1)["author_date"].to_datetime() > date:
            raise ValueError("First revision date is greater than reference date")

        return bisect.bisect_right(
            list(range(1, self.head_revision() + 1)),
            date,
            key=lambda rev_id: self.commit_info(rev_id)["author_date"].to_datetime(),
        )


def get_svn_repo(*args, **kwargs) -> Optional[SvnRepo]:
    """Instantiate an SvnRepo class and trap SubversionException if any raises.
    In case of connection error to the repository, its read access using anonymous
    credentials is also attempted.

    Raises:
        NotFound: if the repository is not found
        SubversionException: if any other kind of subversion problems arise
    """
    credentials = [(None, None), ("anonymous", "anonymous"), ("anonymous", "")]
    for i, (username, password) in enumerate(credentials):
        try:
            if username is not None:
                logger.debug(
                    "Retrying to connect to %s with username '%s' and password '%s'",
                    args[0],
                    username,
                    password,
                )
                kwargs["username"] = username
                kwargs["password"] = password
            return SvnRepo(*args, **kwargs)
        except SubversionException as e:
            connection_error_messages = [
                "Unable to connect to a repository at URL",
                "No provider registered for",
            ]
            error_msgs = [
                "Unknown URL type",
                "is not a working copy",
            ]
            # no more credentials to test, raise NotFound
            if i == len(credentials) - 1:
                raise NotFound(e)
            for msg in error_msgs:
                if msg in e.args[0]:
                    raise NotFound(e)

            if any(
                connection_error_message in e.args[0]
                for connection_error_message in connection_error_messages
            ):
                # still some credentials to test, continue attempting to connect
                # to the repository
                continue
            else:
                raise
    return None
