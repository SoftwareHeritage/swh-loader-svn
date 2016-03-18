# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import svn.remote as remote
import svn.local as local
import tempfile
import uuid

from contextlib import contextmanager

from swh.loader.svn import libloader
from swh.loader.dir import converters, git
from swh.loader.dir.git import GitType


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
    if rev == latest_revision:
        return None

    with cwd(repo['local_url']):
        while rev != latest_revision:
            # checkout to the revision rev
            repo['remote'].checkout(revision=rev, path='.')

            # compute git commit
            parsed_objects = git.walk_and_compute_sha1_from_directory(
                repo['local_url'].encode('utf-8'))

            commit = read_commit(repo, rev)

            yield rev, commit, parsed_objects

            rev += 1


def build_swh_revision(repo_uuid, commit, rev, parsed_objects):
    """Given a svn revision, build a swh revision.

    """
    root_tree = parsed_objects[git.ROOT_TREE_KEY][0]

    author = commit['author_name']
    if author:
        author_committer = {
            'name': author.encode('utf-8'),
            'email': '',
        }
    else:
        author_committer = {
            'name': b'noone',  # HACK
            'email': '',
        }

    msg = commit['message']
    if msg:
        msg = msg.encode('utf-8')
    else:
        msg = b''

    date = {
        'timestamp': commit['author_date'],
        # 'offset': converters.format_to_minutes(commit['author_offset']),
    }

    return {
        'date': date,
        'committer_date': date,
        'type': 'svn',
        'directory': root_tree['sha1_git'],
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
        'parents': [],
    }

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


class SvnLoader(libloader.SvnLoader):
    """A svn loader.

    This will load the svn repository.

    """
    def __init__(self, config):
        super().__init__(config)
        self.log = logging.getLogger('swh.loader.svn.SvnLoader')

    def process(self, svn_url, origin, destination_path):
        """Load a svn repository.

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
            - objects: the actual objects sent to swh storage

        """
        repo = checkout_repo(svn_url, destination_path)

        # 2. retrieve current svn revision

        repo_metadata = repo['local'].info()
        repo['logs'] = read_log_entries(repo)

        latest_revision = repo_metadata['entry_revision']
        repo_uuid = repo_metadata['repository_uuid']

        for rev, commit, parsed_objects in read_svn_revisions(repo, latest_revision):
            swh_revision = build_swh_revision(repo_uuid, commit, rev, parsed_objects)
            self.log.debug('rev: %s, commit: %s, swhrev: %s' % (rev, commit, swh_revision))

        return {'status': True}


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
