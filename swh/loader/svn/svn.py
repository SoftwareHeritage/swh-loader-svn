# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile
import shutil

from subvertpy.ra import RemoteAccess, Auth, get_username_provider
from subvertpy import client, properties

from . import ra

# When log message contains empty data
DEFAULT_AUTHOR_NAME = ''
DEFAULT_AUTHOR_DATE = b''
DEFAULT_AUTHOR_MESSAGE = ''


class SvnRepoException(ValueError):
    def __init__(self, svnrepo, e):
        super().__init__(e)
        self.svnrepo = svnrepo


class SvnRepo():
    """Swh representation of a svn repository.

    """
    def __init__(self, remote_url, origin_id, storage,
                 destination_path=None,
                 with_empty_folder=False,
                 with_extra_commit_line=False):
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
        self.with_empty_folder = with_empty_folder
        self.with_extra_commit_line = with_extra_commit_line

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

    def __to_entry(self, log_entry):
        changed_paths, rev, revprops, has_children = log_entry

        author_date = revprops.get(properties.PROP_REVISION_DATE,
                                   DEFAULT_AUTHOR_DATE)

        author = revprops.get(properties.PROP_REVISION_AUTHOR,
                              DEFAULT_AUTHOR_NAME)

        msg = revprops.get(properties.PROP_REVISION_LOG)
        if msg and self.with_extra_commit_line:
            message = ('%s\n' % msg)
        elif msg:
            message = msg
        else:
            message = DEFAULT_AUTHOR_MESSAGE

        return {
            'rev': rev,
            'author_date': author_date,
            'author_name': author.encode('utf-8'),
            'message': message.encode('utf-8'),
        }

    def logs(self, revision_start, revision_end, block_size=100):
        """Stream svn logs between revision_start and revision_end by chunks of
        block_size logs.

        Yields revision and associated revision information between the
        revision start and revision_end.

        Args:
            revision_start: the svn revision starting bound
            revision_end: the svn revision ending bound
            block_size: block size of revisions to fetch

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

    def fork(self, revision):
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
        # remove_empty_folder = not self.with_empty_folder

        hashes = {}
        for commit in self.logs(start_revision, end_revision):
            rev = commit['rev']
            hashes = ra.compute_or_update_hash_from_replay_at(
                self.conn,
                rev,
                rootpath=self.local_url,
                state=hashes)

            if rev == end_revision:
                nextrev = None
            else:
                nextrev = rev + 1

            yield rev, nextrev, commit, hashes

    def clean_fs(self):
        """Clean up the local working copy.

        """
        shutil.rmtree(self.local_dirname)
