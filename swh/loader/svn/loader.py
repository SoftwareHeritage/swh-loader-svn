# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import pysvn
import tempfile
import subprocess
import sys
import traceback

from contextlib import contextmanager

from swh.core import hashutil
from swh.loader.svn import libloader
from swh.model import git
from swh.model.git import GitType


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


def repo_uuid(local_path):
    with cwd(local_path):
        cmd = 'svn info | grep UUID | cut -f2 -d:'
        uuid = subprocess.check_output(cmd, shell=True)
        return uuid.strip().decode('utf-8')


def init_repo(remote_repo_url, destination_path=None):
    """Initialize a repository without any action on disk.

    Args:
        remote_repo_url: The remote svn url
        destination_path: The optional local parent folder to checkout the
        repository to

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
        local_dirname = tempfile.mkdtemp(suffix='swh.loader',
                                         prefix='tmp.',
                                         dir='/tmp')

    local_repo_url = os.path.join(local_dirname, name)

    client = pysvn.Client()

    return {'client': client,
            'remote_url': remote_repo_url,
            'local_url': local_repo_url}


def fork(repo):
    """Checkout remote repository remote_repo_url to local working copy
    destination_path.

    Args:
        remote_repo_url: The remote svn url
        destination_path: The optional local parent folder to checkout the
        repository to

    Returns:
        Dictionary with the following keys:
            - client: client instance to manipulate the repository
            - uuid: the repository's uuid
            - remote_url: remote url (same as input)
            - local_url: local url which has been computed

    """
    print('svn co %s %s' % (repo['remote_url'], repo['local_url']))
    repo['client'].checkout(repo['remote_url'], repo['local_url'])

    uuid = repo_uuid(repo['local_url'])

    return {'client': repo['client'],
            'uuid': uuid,
            'remote_url': repo['remote_url'],
            'local_url': repo['local_url']}


def checkout(repo, revision):
    """Checkout repository repo at revision.

    Args:
        repo: the repository instance
        revision: the revision number to checkout the repo to.

    """
    repo['client'].checkout(
        repo['remote_url'],
        repo['local_url'],
        revision=pysvn.Revision(pysvn.opt_revision_kind.number, revision))


def read_current_revision_at_path(repo):
    """Retrieve current revision at local path."""
    head_rev = pysvn.Revision(pysvn.opt_revision_kind.head)
    info = repo['client'].info2(repo['local_url'],
                                revision=head_rev,
                                recurse=False)
    return info[0][1]['rev'].number


def read_origin_revision_from_url(repo):
    """Determine the first revision number from which the url appeared.

    """
    return repo['client'].log(repo['remote_url'])[-1].data.get(
        'revision').number


def svn_logs(repo):
    """Return the logs of the repository between the revision start and end of such
    repository.

    """
    return repo['client'].log(
        url_or_path=repo['local_url'],
        revision_start=pysvn.Revision(pysvn.opt_revision_kind.number,
                                      repo['revision_start']),
        revision_end=pysvn.Revision(pysvn.opt_revision_kind.number,
                                    repo['revision_end']))


def check_for_previous_revision(repo, origin_id):
    """Look for possible existing revision.

    Return:
        The previous svn revision known if found, None otherwise.

    """
    storage = repo['storage']
    occ = storage.occurrence_get(origin_id)
    if occ:
        revision_id = occ[0]['target']
        revisions = storage.revision_get([revision_id])
        revision_parents = storage.revision_shortlog([revision_id], limit=1)
        if revisions:
            rev = revisions[0]
            svn_revision = rev['metadata']['extra_headers']['svn_revision']
            return svn_revision, revision_parents

    return None, None


def read_log_entries(repo):
    """Read the logs entries from the repository repo.

    Args
        repo: the repository instance

    Returns
        tuple of revisions and logs.
        revisions: list of revisions in order
        logs: Dictionary with key revision number and value the log entry.
              The log entry is a dictionary with the following keys:
                 - author_date: date of the commit
                 - author_name: name of the author
                 - message: commit message

    """
    logs = {}
    revisions = []
    for log in svn_logs(repo):
        rev = log.revision.number
        revisions.append(rev)
        logs[rev] = {'author_date': log.date,
                     'author_name': log.author,
                     'message': log.message}

    return revisions, logs


def read_svn_revisions(repo):
    """Compute tree for each revision from last known revision to
    latest_revision.

    """
    revisions = repo['revisions']
    logs = repo['logs']
    l = len(revisions)
    for i, rev in enumerate(revisions):
        # checkout to the revision rev
        checkout(repo, revision=rev)

        # compute git commit
        objects_per_path = git.walk_and_compute_sha1_from_directory(
            repo['local_url'].encode('utf-8'),
            dir_ok_fn=lambda dirpath: b'.svn' not in dirpath)

        commit = logs[rev]

        nextrev_index = i+1
        if nextrev_index < l:
            nextrev = revisions[nextrev_index]
        else:
            nextrev = None

        yield rev, nextrev, commit, objects_per_path


def build_swh_revision(repo_uuid, commit, rev, dir_id, parents):
    """Given a svn revision, build a swh revision.

    """
    author = commit['author_name']
    if author:
        author_committer = {
            # HACK: shouldn't we use the same for email?
            'name': author.encode('utf-8'),
            'email': b'',
        }
    else:
        author_committer = {
            'name': b'',  # HACK: some repository have commits without author
            'email': b'',
        }

    msg = commit['message']
    if msg:
        msg = msg.encode('utf-8')
    else:
        msg = b''

    date = {
        'timestamp': commit['author_date'],
        'offset': 0,
    }

    return {
        'date': date,
        'committer_date': date,
        'type': 'svn',
        'directory': dir_id,
        'message': msg,
        'author': author_committer,
        'committer': author_committer,
        'synthetic': True,
        'metadata': {
            'extra_headers': {
                'svn_repo_uuid': repo_uuid,
                'svn_revision': rev,
            }
        },
        'parents': parents,
    }


def build_swh_occurrence(revision_id, origin_id, date):
    """Build a swh occurrence from the revision id, origin id, and date.

    """
    return {'branch': 'master',
            'target': revision_id,
            'target_type': 'revision',
            'origin': origin_id,
            'date': date}


class SvnLoader(libloader.SWHLoader):
    """A svn loader.

    This will load the svn repository.

    """

    def __init__(self, config, log_class=None):
        log_class = 'swh.loader.svn.SvnLoader' if not log_class else log_class
        super().__init__(config, log_class)

    def process(self, svn_url, origin, destination_path):
        """Load a svn repository in swh.

        Checkout the svn repository locally in destination_path.

        Args:
            - svn_url: svn repository url to import
            - origin: Dictionary origin
              - id: origin's id
              - url: url origin we fetched
              - type: type of the origin

        Returns:
            Dictionary with the following keys:
            - status: mandatory, the status result as a boolean
            - stderr: optional when status is True, mandatory otherwise

        """
        repo = init_repo(svn_url, destination_path)
        repo['storage'] = self.storage
        revision_start, revision_parents = check_for_previous_revision(
            repo, origin['id'])

        repo = fork(repo)

        # 2. retrieve current svn revision

        if not revision_start:
            revision_start = read_origin_revision_from_url(repo)

        revision_end = read_current_revision_at_path(repo)

        repo.update({'revision_start': revision_start,
                     'revision_end': revision_end})

        self.log.debug('repo: %s' % repo)

        if revision_start == revision_end and revision_start is not 1:
            self.log.info('%s@%s already injected.' % (svn_url, revision_end))
            return {'status': True}

        revisions, logs = read_log_entries(repo)
        repo.update({
            'revisions': revisions,
            'logs': logs
        })

        self.log.debug('revisions: %s, ..., %s' % (revisions[0],
                                                   revisions[-1]))
        self.log.debug('logs: %s, ..., %s' % (logs[revision_start],
                                              logs[revision_end]))

        repo_uuid = repo['uuid']

        if revision_start == 1:
            parents = {revision_start: []}  # no parents for initial revision
        else:
            parents = {revision_start: revision_parents}

        self.log.debug('parents: %s' % parents)

        swh_revisions = []
        # for each revision
        for rev, nextrev, commit, objects_per_path in read_svn_revisions(repo):
            self.log.debug('rev: %s, nextrev: %s' % (rev, nextrev))

            objects_per_type = {
                GitType.BLOB: [],
                GitType.TREE: [],
                GitType.COMM: [],
                GitType.RELE: [],
                GitType.REFS: [],
            }

            # compute the fs tree's checksums

            dir_id = objects_per_path[git.ROOT_TREE_KEY][0]['sha1_git']
            self.log.debug('tree: %s' % hashutil.hash_to_hex(dir_id))
            swh_revision = build_swh_revision(repo_uuid, commit, rev, dir_id,
                                              parents[rev])
            swh_revision['id'] = git.compute_revision_sha1_git(swh_revision)
            if nextrev:
                parents[nextrev] = [swh_revision['id']]

            # and the revision pointing to that tree
            swh_revisions.append(swh_revision)

            self.log.debug('rev: %s, swhrev: %s' %
                           (rev, hashutil.hash_to_hex(swh_revision['id'])))

            # send blobs
            for tree_path in objects_per_path:
                self.log.debug('tree_path: %s' % tree_path)
                objs = objects_per_path[tree_path]
                for obj in objs:
                    self.log.debug('obj: %s' % obj)
                    objects_per_type[obj['type']].append(obj)

            self.load(objects_per_type, objects_per_path, origin['id'])

        # send revisions and occurrences

        # create occurrence pointing to the latest revision (the last one)
        occ = build_swh_occurrence(swh_revision['id'], origin['id'],
                                   datetime.datetime.utcnow())
        self.log.debug('occ: %s' % occ)

        objects_per_type = {
            GitType.BLOB: [],
            GitType.TREE: [],
            GitType.COMM: swh_revisions,
            GitType.RELE: [],
            GitType.REFS: [occ],
        }

        self.load(objects_per_type, objects_per_path, origin['id'])

        return {'status': True, 'objects': objects_per_type}


class SvnLoaderWithHistory(SvnLoader):
    """A svn loader.

    This will:
    - create the origin if it does not exist
    - open an entry in fetch_history
    - load the svn repository
    - close the entry in fetch_history

    """

    def __init__(self, config):
        super().__init__(config, 'swh.loader.svn.SvnLoaderWithHistory')

    def process(self, svn_url, destination_path):
        """Load a directory in backend.

        Args:
            - svn_url: svn url to import
            - origin: Dictionary origin
              - url: url origin we fetched
              - type: type of the origin

        """
        origin = {'type': 'svn', 'url': svn_url}
        origin['id'] = self.storage.origin_add_one(origin)

        fetch_history_id = self.open_fetch_history(origin['id'])

        try:
            result = super().process(svn_url, origin, destination_path)
        except:
            e_info = sys.exc_info()
            self.log.error('Problem during svn load for repo %s - %s' % (
                svn_url, e_info[1]))
            result = {'status': False, 'stderr': 'reason:%s\ntrace:%s' % (
                    e_info[1],
                    ''.join(traceback.format_tb(e_info[2])))}

        self.close_fetch_history(fetch_history_id, result)
