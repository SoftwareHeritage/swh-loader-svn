# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import os
import svn.remote as remote
import svn.local as local
import tempfile

from contextlib import contextmanager

from swh.core import hashutil
from swh.loader.svn import libloader
from swh.model import git
from swh.model.git import GitType


def checkout_repo(remote_repo_url, destination_path=None):
    """Checkout a remote repository locally.

    Args:
        remote_repo_url: The remote svn url
        destination_path: The optional local parent folder to checkout the
        repository to

    Returns:
        Dictionary with the following keys:
            - remote: remote instance to manipulate the repo
            - local: local instance to manipulate the local repo
            - remote_url: remote url (same as input)
            - local_url: local url which has been computed

    """
    name = os.path.basename(remote_repo_url)
    if destination_path:
        os.makedirs(destination_path, exist_ok=True)
        local_dirname = destination_path
    else:
        local_dirname = tempfile.mkdtemp(suffix='swh.loader.svn.',
                                         prefix='tmp.',
                                         dir='/tmp')

    local_repo_url = os.path.join(local_dirname, name)

    remote_client = remote.RemoteClient(remote_repo_url)
    remote_client.checkout(local_repo_url)

    return {'remote': remote_client,
            'local': local.LocalClient(local_repo_url),
            'remote_url': remote_repo_url,
            'local_url': local_repo_url}


def retrieve_last_known_revision(remote_url_repo):
    """Function that given a remote url returns the last known revision or
    1 if this is the first time.

    """
    # TODO: Contact swh-storage to retrieve the last occurrence for
    # the given origin
    return 1


def read_log_entries(repo):
    """Read the logs entries from the repository.

    """
    logs = {}
    for log in repo['local'].log_default():
        logs[log.revision] = log

    return logs


def read_commit(repo, rev):
    log_entry = repo['logs'][rev]

    return {
        'message': log_entry.msg,
        'author_date': log_entry.date,
        'author_name': log_entry.author,
    }


def read_svn_revisions(repo, latest_revision):
    """Compute tree for each revision from last known revision to
    latest_revision.

    """
    rev = retrieve_last_known_revision(repo['remote_url'])
    if rev != latest_revision:
        with cwd(repo['local_url']):
            while rev != latest_revision:
                # checkout to the revision rev
                repo['remote'].checkout(revision=rev, path='.')

                # compute git commit
                objects_per_path = git.walk_and_compute_sha1_from_directory(
                    repo['local_url'].encode('utf-8'),
                    dir_ok_fn=lambda dirpath: b'.svn' not in dirpath)

                commit = read_commit(repo, rev)

                yield rev, commit, objects_per_path

                rev += 1


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
            'extra-headers': {
                'svn-repo-uuid': repo_uuid,
                'svn-revision': rev,
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


class SvnLoader(libloader.SWHLoader):
    """A svn loader.

    This will load the svn repository.

    """
    def __init__(self, config):
        super().__init__(config)
        self.log = logging.getLogger('swh.loader.svn.SvnLoader')

    def load(self, objects_per_type, objects_per_path, origin_id):
        if self.config['send_contents']:
            self.bulk_send_blobs(objects_per_path,
                                 objects_per_type[GitType.BLOB],
                                 origin_id)
        else:
            self.log.info('Not sending contents')

        if self.config['send_directories']:
            self.bulk_send_trees(objects_per_path,
                                 objects_per_type[GitType.TREE])
        else:
            self.log.info('Not sending directories')

        if self.config['send_revisions']:
            self.bulk_send_commits(objects_per_path,
                                   objects_per_type[GitType.COMM])
        else:
            self.log.info('Not sending revisions')

        if self.config['send_occurrences']:
            self.bulk_send_refs(objects_per_type,
                                objects_per_type[GitType.REFS])
        else:
            self.log.info('Not sending occurrences')

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
        repo = checkout_repo(svn_url, destination_path)

        # 2. retrieve current svn revision

        repo_metadata = repo['local'].info()
        repo['logs'] = read_log_entries(repo)

        latest_revision = repo_metadata['entry_revision']
        repo_uuid = repo_metadata['repository_uuid']

        parents = {1: []}  # rev 1 has no parents

        self.log.debug('repo: %s' % repo)
        self.log.debug('logs: %s' % repo['logs'])

        swh_revisions = []
        for rev, commit, objects_per_path in read_svn_revisions(
                repo, latest_revision):
            dir_id = objects_per_path[git.ROOT_TREE_KEY][0]['sha1_git']
            self.log.debug('tree: %s' % hashutil.hash_to_hex(dir_id))
            swh_revision = build_swh_revision(repo_uuid, commit, rev,
                                              dir_id, parents[rev])
            swh_revision['id'] = git.compute_revision_sha1_git(swh_revision)
            parents[rev+1] = [swh_revision['id']]

            swh_revisions.append(swh_revision)
            self.log.debug('rev: %s, swhrev: %s' % (rev,
                                                    hashutil.hash_to_hex(
                                                        swh_revision['id'])))

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
        super().__init__(config)
        self.log = logging.getLogger('swh.loader.svn.SvnLoaderWithHistory')

    def process(self, svn_url, destination_path):
        """Load a directory in backend.

        Args:
            - svn_url: svn url to import
            - origin: Dictionary origin
              - url: url origin we fetched
              - type: type of the origin

        """
        origin = {'type': 'svn',
                  'url': svn_url}
        origin['id'] = self.storage.origin_add_one(origin)

        fetch_history_id = self.open_fetch_history(origin['id'])

        result = super().process(svn_url, origin, destination_path)

        self.close_fetch_history(fetch_history_id, result)
