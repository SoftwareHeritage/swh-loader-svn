# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pysvn
import tempfile
import shutil

from pysvn import Revision, opt_revision_kind
from retrying import retry

from swh.model import git


# When log message contains empty data
DEFAULT_AUTHOR_NAME = b''
DEFAULT_AUTHOR_DATE = b''
DEFAULT_AUTHOR_MESSAGE = b''


class SvnRepoException(ValueError):
    def __init__(self, svnrepo, e):
        super().__init__(e)
        self.svnrepo = svnrepo


def retry_with_cleanup(exception):
    """Clean the repository from locks before retrying.

    """
    exception.svnrepo.cleanup()
    return True


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

        self.client = pysvn.Client()
        self.local_url = os.path.join(self.local_dirname, local_name)
        self.uuid = None  # Cannot know it yet since we need a working copy
        self.with_empty_folder = with_empty_folder
        self.with_extra_commit_line = with_extra_commit_line

    def __str__(self):
        return str({'remote_url': self.remote_url,
                    'local_url': self.local_url,
                    'uuid': self.uuid,
                    'swh-origin': self.origin_id})

    def cleanup(self):
        """Clean up any locks in the working copy at path.

        """
        self.client.cleanup(self.local_url)

    @retry(retry_on_exception=retry_with_cleanup,
           stop_max_attempt_number=3)
    def checkout(self, revision):
        """Checkout repository repo at revision.

        Args:
            revision: the revision number to checkout the repo to.

        """
        try:
            self.client.checkout(
                self.remote_url,
                self.local_url,
                revision=Revision(opt_revision_kind.number, revision))
        except Exception as e:
            raise SvnRepoException(self, e)

    def fork(self, svn_revision=None):
        """Checkout remote repository to a local working copy (at revision 1
        if the svn revision is not specified).

        This will also update the repository's uuid.

        """
        self.checkout(1 if not svn_revision else svn_revision)
        self.uuid = self.client.info(self.local_url).uuid

    def head_revision(self):
        """Retrieve current revision of the repository's working copy.

        """
        head_rev = Revision(opt_revision_kind.head)
        info = self.client.info2(self.local_url,
                                 revision=head_rev,
                                 recurse=False)
        return info[0][1]['rev'].number

    def initial_revision(self):
        """Retrieve the initial revision from which the remote url appeared.
        Note: This should always be 1 since we won't be dealing with in-depth
        url.

        """
        return self.client.log(self.remote_url)[-1].data.get(
            'revision').number

    def _to_change_paths(self, log_entry):
        """Convert changed paths to dict if any.

        """
        try:
            changed_paths = log_entry.changed_paths
        except AttributeError:
            changed_paths = []

        for paths in changed_paths:
            path = os.path.join(self.local_url, paths.path.lstrip('/'))
            yield {
                'path': path.encode('utf-8'),
                'action': paths.action  # A(dd), M(odified), D(eleted)
            }

    def _to_entry(self, log_entry):
        try:
            author_date = log_entry.date or DEFAULT_AUTHOR_DATE
        except AttributeError:
            author_date = DEFAULT_AUTHOR_DATE

        try:
            author = log_entry.author.encode('utf-8') or DEFAULT_AUTHOR_NAME
        except AttributeError:
            author = DEFAULT_AUTHOR_NAME

        try:

            msg = log_entry.message
            if msg and self.with_extra_commit_line:
                message = ('%s\n' % msg).encode('utf-8')
            elif msg:
                message = msg.encode('utf-8')
            else:
                message = DEFAULT_AUTHOR_MESSAGE

        except AttributeError:
            message = DEFAULT_AUTHOR_MESSAGE

        return {
            'rev': log_entry.revision.number,
            'author_date': author_date,
            'author_name': author,
            'message': message,
            'changed_paths': self._to_change_paths(log_entry),
        }

    @retry(stop_max_attempt_number=3)
    def _logs(self, revision_start, revision_end):
        rev_start = Revision(opt_revision_kind.number, revision_start)
        rev_end = Revision(opt_revision_kind.number, revision_end)
        return self.client.log(url_or_path=self.local_url,
                               revision_start=rev_start,
                               revision_end=rev_end,
                               discover_changed_paths=True)

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
        r1 = revision_start
        r2 = r1 + block_size - 1

        done = False
        if r2 >= revision_end:
            r2 = revision_end
            done = True

        for log_entry in self._logs(r1, r2):
            yield self._to_entry(log_entry)

        if not done:
            yield from self.logs(r2 + 1, revision_end, block_size)

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
        if not self.with_empty_folder:
            def dir_ok_fn(dirpath):
                return b'.svn' not in dirpath and len(os.listdir(dirpath)) > 0
        else:
            def dir_ok_fn(dirpath):
                return b'.svn' not in dirpath

        local_url = self.local_url.encode('utf-8')
        for commit in self.logs(start_revision, end_revision):
            rev = commit['rev']
            if rev == start_revision:  # first time, we walk the complete tree
                objects_per_path = git.walk_and_compute_sha1_from_directory(
                    local_url,
                    dir_ok_fn=dir_ok_fn)
            else:
                # checkout to the next revision rev
                self.checkout(revision=rev)
                # and we update  only what needs to be
                objects_per_path = git.update_checksums_from(
                    commit['changed_paths'],
                    objects_per_path,
                    dir_ok_fn=dir_ok_fn)

            if rev == end_revision:
                nextrev = None
            else:
                nextrev = rev + 1

            yield rev, nextrev, commit, objects_per_path

    def clean_fs(self):
        """Clean up the local working copy.

        """
        shutil.rmtree(self.local_dirname)
