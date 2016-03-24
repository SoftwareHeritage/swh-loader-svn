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


def fork(remote_repo_url, destination_path=None):
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
    name = os.path.basename(remote_repo_url)
    if destination_path:
        os.makedirs(destination_path, exist_ok=True)
        local_dirname = destination_path
    else:
        local_dirname = tempfile.mkdtemp(suffix='swh.loader',
                                         prefix='tmp.',
                                         dir='/tmp')

    print('svn co %s %s' % (remote_repo_url, local_dirname))
    local_repo_url = os.path.join(local_dirname, name)

    client = pysvn.Client()
    client.checkout(remote_repo_url, local_repo_url)

    uuid = repo_uuid(local_repo_url)

    return {'client': client,
            'uuid': uuid,
            'remote_url': remote_repo_url,
            'local_url': local_repo_url}


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
    """Return the logs of the repository between the revision start and end of such repository.

    """
    return repo['client'].log(
        url_or_path=repo['local_url'],
        revision_start=pysvn.Revision(pysvn.opt_revision_kind.number,
                                      repo['revision_start']),
        revision_end=pysvn.Revision(pysvn.opt_revision_kind.number,
                                    repo['revision_end']))


def retrieve_last_known_revision(repo, from_start=True):  # hack
    """Given a repo, returns the last swh known revision or its original revision if
    this is the first time.

    """
    if from_start:
        return read_origin_revision_from_url(repo)
    # TODO: Contact swh-storage to retrieve the last occurrence for
    # the given origin
    return 1


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
    rev = repo['revision_start']
    revision_end = repo['revision_end']
    logs = repo['logs']
    l = len(revisions)
    if rev != revision_end:
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
        'offset': 0,  # HACK: PySvn transforms into datetime with utc timezone
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
        repo = fork(svn_url, destination_path)

        # 2. retrieve current svn revision

        revision_start = retrieve_last_known_revision(repo)
        revision_end = read_current_revision_at_path(repo)

        repo.update({'revision_start': revision_start,
                     'revision_end': revision_end})

        self.log.debug('repo: %s' % repo)
        revisions, logs = read_log_entries(repo)
        repo['revisions'] = revisions
        repo['logs'] = logs

        self.log.debug(repo['logs'])

        repo_uuid = repo['uuid']

        parents = {revision_start: []}  # rev 1 has no parents

        swh_revisions = []
        for rev, nextrev, commit, objects_per_path in read_svn_revisions(repo):
            self.log.debug('rev: %s' % rev)

            dir_id = objects_per_path[git.ROOT_TREE_KEY][0]['sha1_git']
            self.log.debug('tree: %s' % hashutil.hash_to_hex(dir_id))
            swh_revision = build_swh_revision(repo_uuid, commit, rev, dir_id,
                                              parents[rev])
            swh_revision['id'] = git.compute_revision_sha1_git(swh_revision)
            if nextrev:
                parents[nextrev] = [swh_revision['id']]

            swh_revisions.append(swh_revision)
            self.log.debug('rev: %s, swhrev: %s' %
                           (rev, hashutil.hash_to_hex(swh_revision['id'])))

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
        for tree_path in objects_per_path:
            objs = objects_per_path[tree_path]
            for obj in objs:
                objects_per_type[obj['type']].append(obj)

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
            self.log.error('Problem during svn load for repo %s - %s' % (svn_url, e_info[1]))
            result = {'status': False, 'stderr': 'reason:%s\ntrace:%s' % (
                    e_info[1],
                    ''.join(traceback.format_tb(e_info[2])))}

        self.close_fetch_history(fetch_history_id, result)
