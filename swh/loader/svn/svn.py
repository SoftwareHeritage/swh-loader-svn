# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""SVN client in charge of iterating over svn logs and yield commit
representations including the hash tree/content computations per
svn commit.

"""

import os
import tempfile
import shutil

from subvertpy.ra import RemoteAccess, Auth, get_username_provider
from subvertpy import client, properties

from swh.model import git

from . import ra, utils, converters

# When log message contains empty data
DEFAULT_AUTHOR_MESSAGE = ''


class SvnRepoException(ValueError):
    def __init__(self, svnrepo, e):
        super().__init__(e)
        self.svnrepo = svnrepo


class SvnRepo():
    """Swh representation of a svn repository.

    To override some of the behavior regarding the log properties, you
    can instantiate a subclass of this class and override:
    - def convert_commit_author(self, author)
    - def convert_commit_message(self, msg)
    - def convert_commit_date(self, date)

    cf. SvnRepoWithExtraCommitLine for an example.

    """
    def __init__(self, remote_url, origin_id, storage,
                 destination_path=None,
                 with_empty_folder=False):
        self.remote_url = remote_url.rstrip('/')
        self.storage = storage
        self.origin_id = origin_id

        if destination_path:
            os.makedirs(destination_path, exist_ok=True)
            root_dir = destination_path
        else:
            root_dir = '/tmp'

        self.local_dirname = tempfile.mkdtemp(suffix='.swh.loader',
                                              prefix='tmp.',
                                              dir=root_dir)

        local_name = os.path.basename(self.remote_url)

        auth = Auth([get_username_provider()])
        # one connection for log iteration
        self.conn_log = RemoteAccess(self.remote_url,
                                     auth=auth)
        # another for replay
        self.conn = RemoteAccess(self.remote_url,
                                 auth=auth)
        # one client for update operation
        self.client = client.Client(auth=auth)

        self.local_url = os.path.join(self.local_dirname, local_name).encode(
            'utf-8')
        self.uuid = self.conn.get_uuid().encode('utf-8')

        # In charge of computing hash while replaying svn logs
        self.with_empty_folder = with_empty_folder
        self.swhreplay = self._init_swhreplay()

    def _init_swhreplay(self, state=None):
        if self.with_empty_folder:
            return ra.SWHReplay(
                conn=self.conn,
                rootpath=self.local_url,
                state=state)
        return ra.SWHReplayNoEmptyFolder(
            conn=self.conn,
            rootpath=self.local_url,
            state=state)

    def __str__(self):
        return str({'remote_url': self.remote_url,
                    'local_url': self.local_url,
                    'uuid': self.uuid,
                    'swh-origin': self.origin_id})

    def head_revision(self):
        """Retrieve current revision of the repository's working copy.

        """
        return self.conn.get_latest_revnum()

    def initial_revision(self):
        """Retrieve the initial revision from which the remote url appeared.
        Note: This should always be 1 since we won't be dealing with in-depth
        url.

        """
        return 1

    def convert_commit_message(self, msg):
        """Do something with message (e.g add extra line, etc...)

        cf. SvnRepo for a simple implementation.

        Args:
            msg (str): the commit message to convert.

        Returns:
            The transformed message as bytes.

        """
        return msg.encode('utf-8')

    def convert_commit_date(self, date):
        """Convert the message date (e.g, convert into timestamp or whatever
        makes sense to you.).

           Args:
               date (str): the commit date to convert.

            Returns:
               The transformed date.

        """
        return utils.strdate_to_timestamp(date)

    def convert_commit_author(self, author):
        """Convert the commit author (e.g, convert into dict or whatever
        makes sense to you.).

        Args:
            author (str): the commit author to convert.

        Returns:
            The transformed author as dict.

        """
        return converters.svn_author_to_person(author, self.uuid)

    def __to_entry(self, log_entry):
        changed_paths, rev, revprops, has_children = log_entry

        author_date = self.convert_commit_date(
            revprops.get(properties.PROP_REVISION_DATE))

        author = self.convert_commit_author(
            revprops.get(properties.PROP_REVISION_AUTHOR))

        message = self.convert_commit_message(
            revprops.get(properties.PROP_REVISION_LOG,
                         DEFAULT_AUTHOR_MESSAGE))

        return {
            'rev': rev,
            'author_date': author_date,
            'author_name': author,
            'message': message,
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
            tuple of revisions and logs.
            revisions: list of revisions in order
            logs: Dictionary with key revision number and value the log entry.
                  The log entry is a dictionary with the following keys:
                     - author_date: date of the commit
                     - author_name: name of the author
                     - message: commit message

        """
        for log_entry in self.conn_log.iter_log(paths=None,
                                                start=revision_start,
                                                end=revision_end,
                                                discover_changed_paths=False):
            yield self.__to_entry(log_entry)

    def export(self, revision):
        """Export the repository to a given version.

        """
        self.client.export(self.remote_url,
                           to=self.local_url.decode('utf-8'),
                           rev=revision)

    def swh_previous_revision(self):
        """Look for possible existing revision in swh.

        Returns:
            The previous swh revision if found, None otherwise.

        """
        storage = self.storage
        occ = storage.occurrence_get(self.origin_id)
        if occ:
            revision_id = occ[0]['target']
            revisions = storage.revision_get([revision_id])

            if revisions:
                return revisions[0]

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
        hashes = {}
        for commit in self.logs(start_revision, end_revision):
            rev = commit['rev']
            hashes = self.swhreplay.compute_hashes(rev)

            if rev == end_revision:
                nextrev = None
            else:
                nextrev = rev + 1

            yield rev, nextrev, commit, hashes

    def swh_hash_data_at_revision(self, revision):
        """Compute the hash data at revision.

        Expected to be used for update only.

        """
        # Update the disk at revision
        self.export(revision)
        # Compute the current hashes on disk
        hashes = git.compute_hashes_from_directory(
            self.local_url,
            remove_empty_folder=not self.with_empty_folder)

        hashes = utils.convert_hashes_with_relative_path(
            hashes,
            rootpath=self.local_url)

        # Update the replay collaborator with the right state
        self.swhreplay = self._init_swhreplay(state=hashes)

        # Retrieve the commit information for revision
        commit = list(self.logs(revision, revision))[0]

        yield revision, revision + 1, commit, hashes

    def clean_fs(self):
        """Clean up the local working copy.

        """
        shutil.rmtree(self.local_dirname)


class SvnRepoWithExtraCommitLine(SvnRepo):
    """This class does exactly as BaseSvnRepo except for the commit
    message which is extended with a new line.

    """
    def convert_commit_message(self, msg):
        """Add an extra line to the commit message and encode in bytes.

        Args:
            msg (str): the commit message to convert_commit_message

        Returns:
            The transformed message.
        """
        return ('%s\n' % msg).encode('utf-8')
