# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""SVN client in charge of iterating over svn logs and yield commit
representations including the hash tree/content computations per svn
commit.

"""
import logging
import os
import shutil
import tempfile
from typing import Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote, urlparse, urlunparse

from subvertpy import SubversionException, client, properties, wc
from subvertpy.ra import (
    Auth,
    RemoteAccess,
    get_simple_prompt_provider,
    get_username_provider,
)

from swh.model.from_disk import Directory as DirectoryFromDisk
from swh.model.model import (
    Content,
    Directory,
    Person,
    SkippedContent,
    TimestampWithTimezone,
)

from . import converters, replay
from .svn_retry import svn_retry
from .utils import is_recursive_external, parse_external_definition

# When log message contains empty data
DEFAULT_AUTHOR_MESSAGE = ""

logger = logging.getLogger(__name__)


def quote_svn_url(url: str) -> str:
    return quote(url, safe="/:!$&'()*+,=@")


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
        origin_url: str,
        local_dirname: str,
        max_content_length: int,
        from_dump: bool = False,
        debug: bool = False,
    ):
        self.origin_url = origin_url
        self.from_dump = from_dump

        # default auth provider for anonymous access
        auth_providers = [get_username_provider()]

        # check if basic auth is required
        parsed_origin_url = urlparse(origin_url)
        self.username = parsed_origin_url.username or ""
        self.password = parsed_origin_url.password or ""
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

        auth = Auth(auth_providers)
        # one client for update operation
        self.client = client.Client(auth=auth)

        if not self.remote_url.startswith("file://"):
            # use redirection URL if any for remote operations
            self.remote_url = self.info(self.remote_url).url

        # one connection for log iteration
        self.conn_log = self.remote_access(auth)
        # another for replay
        self.conn = self.remote_access(auth)

        if not self.from_dump:
            self.remote_url = self.info(self.remote_url).repos_root_url

        self.local_dirname = local_dirname
        local_name = os.path.basename(self.remote_url)
        self.local_url = os.path.join(self.local_dirname, local_name).encode("utf-8")

        self.uuid = self.conn.get_uuid().encode("utf-8")
        self.swhreplay = replay.Replay(
            conn=self.conn,
            rootpath=self.local_url,
            svnrepo=self,
            temp_dir=local_dirname,
            debug=debug,
        )
        self.max_content_length = max_content_length
        self.has_relative_externals = False
        self.has_recursive_externals = False
        self.replay_started = False

        # compute root directory path from the remote repository URL, required to
        # properly load the sub-tree of a repository mounted from a dump file
        repos_root_url = self.info(self.origin_url).repos_root_url
        self.root_directory = self.origin_url.rstrip("/").replace(repos_root_url, "", 1)

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
        return self.conn.get_latest_revnum()

    def initial_revision(self) -> int:
        """Retrieve the initial revision from which the remote url appeared."""
        return 1

    def convert_commit_message(self, msg: Union[str, bytes]) -> bytes:
        """Simply encode the commit message.

        Args:
            msg: the commit message to convert.

        Returns:
            The transformed message as bytes.

        """
        if isinstance(msg, bytes):
            return msg
        return msg.encode("utf-8")

    def convert_commit_date(self, date: bytes) -> TimestampWithTimezone:
        """Convert the message commit date into a timestamp in swh format.
        The precision is kept.

        Args:
            date: the commit date to convert.

        Returns:
            The transformed date.

        """
        return converters.svn_date_to_swh_date(date)

    def convert_commit_author(self, author: Optional[bytes]) -> Person:
        """Convert the commit author into an swh person.

        Args:
            author: the commit author to convert.

        Returns:
            Person as model object

        """
        return converters.svn_author_to_swh_person(author)

    def __to_entry(self, log_entry: Tuple) -> Dict:
        changed_paths, rev, revprops, has_children = log_entry

        author_date = self.convert_commit_date(
            revprops.get(properties.PROP_REVISION_DATE)
        )

        author = self.convert_commit_author(
            revprops.get(properties.PROP_REVISION_AUTHOR)
        )

        message = self.convert_commit_message(
            revprops.get(properties.PROP_REVISION_LOG, DEFAULT_AUTHOR_MESSAGE)
        )

        has_changes = (
            not self.from_dump
            or changed_paths is not None
            and any(
                changed_path.startswith(self.root_directory)
                for changed_path in changed_paths.keys()
            )
        )

        return {
            "rev": rev,
            "author_date": author_date,
            "author_name": author,
            "message": message,
            "has_changes": has_changes,
            "changed_paths": changed_paths,
        }

    def logs(self, revision_start: int, revision_end: int) -> Iterator[Dict]:
        """Stream svn logs between revision_start and revision_end by chunks of
        block_size logs.

        Yields revision and associated revision information between the
        revision start and revision_end.

        Args:
            revision_start: the svn revision starting bound
            revision_end: the svn revision ending bound

        Yields:
            tuple: tuple of revisions and logs:

                - revisions: list of revisions in order
                - logs: Dictionary with key revision number and value the log
                  entry. The log entry is a dictionary with the following keys:

                    - author_date: date of the commit
                    - author_name: name of the author
                    - message: commit message

        """
        for log_entry in self.conn_log.iter_log(
            paths=None,
            start=revision_start,
            end=revision_end,
            discover_changed_paths=True,
        ):
            yield self.__to_entry(log_entry)

    @svn_retry()
    def commit_info(self, revision: int) -> Optional[Dict]:
        """Return commit information.

        Args:
            revision: svn revision to return commit info

        Returns:
            A dictionary filled with commit info, see :meth:`swh.loader.svn.svn.logs`
            for details about its content.
        """
        return next(self.logs(revision, revision), None)

    @svn_retry()
    def remote_access(self, auth: Auth) -> RemoteAccess:
        """Simple wrapper around subvertpy.ra.RemoteAccess creation
        enabling to retry the operation if a network error occurs."""
        return RemoteAccess(self.remote_url, auth=auth)

    @svn_retry()
    def info(self, origin_url: str):
        """Simple wrapper around subvertpy.client.Client.info enabling to retry
        the command if a network error occurs."""
        info = self.client.info(quote_svn_url(origin_url).rstrip("/"))
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
    ) -> int:
        """Simple wrapper around subvertpy.client.Client.export enabling to retry
        the command if a network error occurs.

        See documentation of svn_client_export5 function from subversion C API
        to get details about parameters.
        """
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
            # the right export URL,recursive externals are also checked

            # get all svn:externals properties recursively
            externals = self.propget(
                "svn:externals", self.remote_url, revision, revision, True
            )
            self.has_relative_externals = False
            self.has_recursive_externals = False
            for path, external_defs in externals.items():
                if self.has_relative_externals or self.has_recursive_externals:
                    break
                path = path.replace(self.remote_url.rstrip("/") + "/", "")
                for external_def in os.fsdecode(external_defs).split("\n"):
                    # skip empty line or comment
                    if not external_def or external_def.startswith("#"):
                        continue
                    (
                        external_path,
                        external_url,
                        _,
                        relative_url,
                    ) = parse_external_definition(
                        external_def.rstrip("\r"), path, self.origin_url
                    )

                    if is_recursive_external(
                        self.origin_url,
                        path,
                        external_path,
                        external_url,
                    ):
                        self.has_recursive_externals = True
                        url = self.remote_url
                        break

                    if relative_url:
                        self.has_relative_externals = True
                        url = self.origin_url
                        break

        try:
            url = url.rstrip("/")

            self.export(
                url,
                to=local_url,
                rev=revision,
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
