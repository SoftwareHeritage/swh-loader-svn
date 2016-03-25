# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pysvn
import tempfile
import subprocess

from contextlib import contextmanager

from swh.model import git


@contextmanager
def cwd(path):
    """Contextually change the working directory to do thy bidding.
    Then gets back to the original location.

    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def init_repo(remote_repo_url, destination_path=None):
    """Initialize a repository without any svn action on disk. There may be
    temporary folder creation on disk as side effect (if destination_path is
    not provided)

    Args:
        remote_repo_url: The remote svn url
        destination_path: The optional local parent folder to checkout the
        repository to.

    Returns:
        Dictionary with the following keys:
            - client: client instance to manipulate the repository
            - remote_url: remote url (same as input)
            - local_url: local url which has been computed

    """
    name = os.path.basename(remote_repo_url)
    if destination_path:
        os.makedirs(destination_path, exist_ok=True)
        local_dirname = destination_path
    else:
        local_dirname = tempfile.mkdtemp(suffix='.swh.loader',
                                         prefix='tmp.',
                                         dir='/tmp')

    local_repo_url = os.path.join(local_dirname, name)

    client = pysvn.Client()

    return {'client': client,
            'remote_url': remote_repo_url,
            'local_url': local_repo_url}


class SvnRepo():
    """Swh representation of a svn repository.

    """

    def __init__(self, remote_url, origin_id, storage, local_url=None):
        self.remote_url = remote_url
        self.storage = storage
        self.origin_id = origin_id

        r = init_repo(remote_url, local_url)
        self.client = r['client']
        self.local_url = r['local_url']
        self.uuid = None

    def __str__(self):
        return str({'remote_url': self.remote_url,
                    'local_url': self.local_url,
                    'uuid': self.uuid,
                    'swh-origin': self.origin_id})

    def read_uuid(self):
        with cwd(self.local_url):
            cmd = 'svn info | grep UUID | cut -f2 -d:'
            uuid = subprocess.check_output(cmd, shell=True)
            return uuid.strip().decode('utf-8')

    def checkout(self, revision):
        """Checkout repository repo at revision.

        Args:
            revision: the revision number to checkout the repo to.

        """
        self.client.checkout(
            self.remote_url,
            self.local_url,
            revision=pysvn.Revision(pysvn.opt_revision_kind.number, revision))

    def fork(self):
        """Checkout remote repository to a local working copy (at revision 1).

        This will also update the repository's uuid.

        """
        self.checkout(1)
        self.uuid = self.read_uuid()

    def head_revision(self):
        """Retrieve current revision of the repository's working copy.

        """
        head_rev = pysvn.Revision(pysvn.opt_revision_kind.head)
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

    def stream_logs(self, revision_start, revision_end, block_size=100):
        """Stream svn logs between revision_start and revision_end by chunks of block_size logs.

        """
        r1 = revision_start
        done = False
        r2 = r1 + block_size
        if r2 > revision_end:
            r2 = revision_end
            done = True

        for l in self.client.log(url_or_path=self.local_url,
                                 revision_start=pysvn.Revision(pysvn.opt_revision_kind.number, r1),
                                 revision_end=pysvn.Revision(pysvn.opt_revision_kind.number, r2)):
            yield l

        if not done:
            yield from self.stream_logs(r2, revision_end)

    def logs(self, revision_start, revision_end):
        """Yields revision and associated revision information between the revision start
        and revision_end.

        Args:
            repo: the repository instance

        Yields:
            tuple of revisions and logs.
            revisions: list of revisions in order
            logs: Dictionary with key revision number and value the log entry.
                  The log entry is a dictionary with the following keys:
                     - author_date: date of the commit
                     - author_name: name of the author
                     - message: commit message

        """
        for log in self.stream_logs(revision_start, revision_end):
            yield log.revision.number, {'author_date': log.date,
                                        'author_name': log.author,
                                        'message': log.message}

    def swh_previous_revision_and_parents(self):
        """Look for possible existing revision.

        Returns:
            The tuple (previous svn revision known by swh and its parents) if
            the svn revision is found, the tuple (None, None) otherwise.

        """
        storage = self.storage
        occ = storage.occurrence_get(self.origin_id)
        if occ:
            revision_id = occ[0]['target']
            revisions = storage.revision_get([revision_id])
            parents = {}
            for rev, pparents in storage.revision_shortlog([revision_id],
                                                           limit=1):
                parents[rev] = pparents

            if revisions:
                rev = revisions[0]
                svn_revision = rev['metadata']['extra_headers']['svn_revision']
                return svn_revision, parents[revision_id]

        return None, None

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
        local_url = self.local_url.encode('utf-8')
        for rev, commit in self.logs(start_revision, end_revision):
            # checkout to the revision rev
            self.checkout(revision=rev)

            # compute git commit
            objects_per_path = git.walk_and_compute_sha1_from_directory(
                local_url, dir_ok_fn=lambda dirpath: b'.svn' not in dirpath)

            if rev == end_revision:
                nextrev = None
            else:
                nextrev = rev + 1

            yield rev, nextrev, commit, objects_per_path
