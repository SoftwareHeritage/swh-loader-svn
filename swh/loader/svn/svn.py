# Copyright (C) 2015-2020  The Software Heritage developers
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

from subvertpy import client, properties
from subvertpy.ra import Auth, RemoteAccess, get_username_provider

from swh.model.from_disk import Directory

from . import converters, ra

# When log message contains empty data
DEFAULT_AUTHOR_MESSAGE = ""


class SvnRepo:
    """Svn repository representation.

    Args:
        remote_url (str):
        origin_url (str): Associated origin identifier
        local_dirname (str): Path to write intermediary svn action results

    """

    def __init__(self, remote_url, origin_url, local_dirname, max_content_length):
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
        self.swhreplay = ra.Replay(conn=self.conn, rootpath=self.local_url)
        self.max_content_length = max_content_length

    def __str__(self):
        return str(
            {
                "swh-origin": self.origin_url,
                "remote_url": self.remote_url,
                "local_url": self.local_url,
                "uuid": self.uuid,
            }
        )

    def head_revision(self):
        """Retrieve current head revision.

        """
        return self.conn.get_latest_revnum()

    def initial_revision(self):
        """Retrieve the initial revision from which the remote url appeared.

        """
        return 1

    def convert_commit_message(self, msg):
        """Simply encode the commit message.

        Args:
            msg (str): the commit message to convert.

        Returns:
            The transformed message as bytes.

        """
        if isinstance(msg, bytes):
            return msg
        return msg.encode("utf-8")

    def convert_commit_date(self, date):
        """Convert the message commit date into a timestamp in swh format.
        The precision is kept.

        Args:
            date (str): the commit date to convert.

        Returns:
            The transformed date.

        """
        return converters.svn_date_to_swh_date(date)

    def convert_commit_author(self, author):
        """Convert the commit author into an swh person.

        Args:
            author (str): the commit author to convert.

        Returns:
            Person: a model object

        """
        return converters.svn_author_to_swh_person(author)

    def __to_entry(self, log_entry):
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

    def logs(self, revision_start, revision_end):
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

    def export(self, revision):
        """Export the repository to a given version.

        """
        self.client.export(
            self.remote_url,
            to=self.local_url.decode("utf-8"),
            rev=revision,
            ignore_keywords=True,
        )

    def export_temporary(self, revision):
        """Export the repository to a given revision in a temporary location.
        This is up to the caller of this function to clean up the
        temporary location when done (cf. self.clean_fs method)

        Args:
            revision: Revision to export at

        Returns:
            The tuple local_dirname the temporary location root
            folder, local_url where the repository was exported.

        """
        local_dirname = tempfile.mkdtemp(
            prefix="check-revision-%s." % revision, dir=self.local_dirname
        )
        local_name = os.path.basename(self.remote_url)
        local_url = os.path.join(local_dirname, local_name)
        self.client.export(
            self.remote_url, to=local_url, rev=revision, ignore_keywords=True
        )
        return local_dirname, os.fsencode(local_url)

    def swh_hash_data_per_revision(self, start_revision, end_revision):
        """Compute swh hash data per each revision between start_revision and
        end_revision.

        Args:
            start_revision: starting revision
            end_revision: ending revision

        Yields:
            tuple (rev, nextrev, commit, objects_per_path)
            - rev: current revision
            - nextrev: next revision
            - commit: commit data (author, date, message) for such revision
            - objects_per_path: dictionary of path, swh hash data with type

        """
        for commit in self.logs(start_revision, end_revision):
            rev = commit["rev"]
            objects = self.swhreplay.compute_objects(rev)

            if rev == end_revision:
                nextrev = None
            else:
                nextrev = rev + 1

            yield rev, nextrev, commit, objects, self.swhreplay.directory

    def swh_hash_data_at_revision(self, revision):
        """Compute the hash data at revision.

        Expected to be used for update only.

        """
        # Update the disk at revision
        self.export(revision)
        # Compute the current hashes on disk
        directory = Directory.from_disk(
            path=os.fsencode(self.local_url), max_content_length=self.max_content_length
        )

        # Update the replay collaborator with the right state
        self.swhreplay = ra.Replay(
            conn=self.conn, rootpath=self.local_url, directory=directory
        )

        # Retrieve the commit information for revision
        commit = list(self.logs(revision, revision))[0]

        yield revision, revision + 1, commit, {}, directory

    def clean_fs(self, local_dirname=None):
        """Clean up the local working copy.

        Args:
            local_dirname (str): Path to remove recursively if
            provided. Otherwise, remove the temporary upper root tree
            used for svn repository loading.

        """
        dirname = local_dirname if local_dirname else self.local_dirname
        if os.path.exists(dirname):
            logging.debug("cleanup %s" % dirname)
            shutil.rmtree(dirname)
