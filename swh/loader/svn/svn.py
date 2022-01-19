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

from subvertpy import client, properties
from subvertpy.ra import Auth, RemoteAccess, get_username_provider

from swh.model.from_disk import Directory as DirectoryFromDisk
from swh.model.model import (
    Content,
    Directory,
    Person,
    SkippedContent,
    TimestampWithTimezone,
)

from . import converters, replay
from .utils import parse_external_definition

# When log message contains empty data
DEFAULT_AUTHOR_MESSAGE = ""


logger = logging.getLogger(__name__)


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
    ):
        self.remote_url = remote_url.rstrip("/")
        self.origin_url = origin_url

        auth = Auth([get_username_provider()])
        # one connection for log iteration
        self.conn_log = RemoteAccess(self.remote_url, auth=auth)
        # another for replay
        self.conn = RemoteAccess(self.remote_url, auth=auth)
        # one client for update operation
        self.client = client.Client(auth=auth)

        self.local_dirname = local_dirname
        local_name = os.path.basename(self.remote_url)
        self.local_url = os.path.join(self.local_dirname, local_name).encode("utf-8")

        self.uuid = self.conn.get_uuid().encode("utf-8")
        self.swhreplay = replay.Replay(
            conn=self.conn,
            rootpath=self.local_url,
            svnrepo=self,
            temp_dir=local_dirname,
        )
        self.max_content_length = max_content_length
        self.has_relative_externals = False
        self.replay_started = False

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
        """Retrieve current head revision.

        """
        return self.conn.get_latest_revnum()

    def initial_revision(self) -> int:
        """Retrieve the initial revision from which the remote url appeared.

        """
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

        return {
            "rev": rev,
            "author_date": author_date,
            "author_name": author,
            "message": message,
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
            discover_changed_paths=False,
        ):
            yield self.__to_entry(log_entry)

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
        # paths oustide it, we need to export from the origin URL as the remote URL can
        # target a dump mounted on the local filesystem
        if self.replay_started and self.has_relative_externals:
            # externals detected while replaying revisions
            url = self.origin_url
        elif not self.replay_started and self.remote_url.startswith("file://"):
            # revisions replay has not started, we need to check if svn:externals
            # properties are set from a checkout of the revision and if some
            # external URLs are relative to pick the right export URL
            with tempfile.TemporaryDirectory(
                dir=self.local_dirname, prefix=f"checkout-revision-{revision}."
            ) as co_dirname:
                self.client.checkout(
                    self.remote_url, co_dirname, revision, ignore_externals=True
                )
                # get all svn:externals properties recursively
                externals = self.client.propget(
                    "svn:externals", co_dirname, None, revision, True
                )
                self.has_relative_externals = False
                for path, external_defs in externals.items():
                    if self.has_relative_externals:
                        break
                    for external_def in os.fsdecode(external_defs).split("\n"):
                        # skip empty line or comment
                        if not external_def or external_def.startswith("#"):
                            continue
                        _, _, _, relative_url = parse_external_definition(
                            external_def.rstrip("\r"), path, self.origin_url
                        )
                        if relative_url:
                            self.has_relative_externals = True
                            url = self.origin_url
                            break

        self.client.export(
            url.rstrip("/"), to=local_url, rev=revision, ignore_keywords=True,
        )
        return local_dirname, os.fsencode(local_url)

    def swh_hash_data_per_revision(
        self, start_revision: int, end_revision: int
    ) -> Iterator[
        Tuple[
            int,
            Optional[int],
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
            - nextrev: next revision or None if we reached end_revision.
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
            objects = self.swhreplay.compute_objects(rev)

            if rev == end_revision:
                nextrev = None
            else:
                nextrev = rev + 1

            if rev >= start_revision:
                # start yielding new data to archive once we reached the revision to
                # resume the loading from
                yield rev, nextrev, commit, objects, self.swhreplay.directory

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
        commit = list(self.logs(revision, revision))[0]

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
