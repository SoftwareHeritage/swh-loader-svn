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

from swh.core import hashutil
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


def svn_tree_at(repo, latest_revision):
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
            # print(parsed_objects)

            yield rev, parsed_objects

            rev += 1


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

    def list_repo_objs(self, dir_path, revision, release):
        """List all objects from dir_path.

        Args:
            - dir_path (path): the directory to list
            - revision: revision dictionary representation
            - release: release dictionary representation

        Returns:
            a dict containing lists of `Oid`s with keys for each object type:
            - CONTENT
            - DIRECTORY
        """
        def get_objects_per_object_type(objects_per_path):
            m = {
                GitType.BLOB: [],
                GitType.TREE: [],
                GitType.COMM: [],
                GitType.RELE: []
            }
            for tree_path in objects_per_path:
                objs = objects_per_path[tree_path]
                for obj in objs:
                    m[obj['type']].append(obj)

            return m

        def _revision_from(tree_hash, revision, objects):
            full_rev = dict(revision)
            full_rev['directory'] = tree_hash
            full_rev = converters.commit_to_revision(full_rev, objects)
            full_rev['id'] = git.compute_revision_sha1_git(full_rev)
            return full_rev

        def _release_from(revision_hash, release):
            full_rel = dict(release)
            full_rel['target'] = revision_hash
            full_rel['target_type'] = 'revision'
            full_rel = converters.annotated_tag_to_release(full_rel)
            full_rel['id'] = git.compute_release_sha1_git(full_rel)
            return full_rel

        log_id = str(uuid.uuid4())
        sdir_path = dir_path.decode('utf-8')

        self.log.info("Started listing %s" % dir_path, extra={
            'swh_type': 'dir_list_objs_start',
            'swh_repo': sdir_path,
            'swh_id': log_id,
        })

        objects_per_path = git.walk_and_compute_sha1_from_directory(dir_path)

        objects = get_objects_per_object_type(objects_per_path)

        tree_hash = objects_per_path[git.ROOT_TREE_KEY][0]['sha1_git']

        full_rev = _revision_from(tree_hash, revision, objects_per_path)

        objects[GitType.COMM] = [full_rev]

        if release and 'name' in release:
            full_rel = _release_from(full_rev['id'], release)
            objects[GitType.RELE] = [full_rel]

        self.log.info("Done listing the objects in %s: %d contents, "
                      "%d directories, %d revisions, %d releases" % (
                          sdir_path,
                          len(objects[GitType.BLOB]),
                          len(objects[GitType.TREE]),
                          len(objects[GitType.COMM]),
                          len(objects[GitType.RELE])
                      ), extra={
                          'swh_type': 'dir_list_objs_end',
                          'swh_repo': sdir_path,
                          'swh_num_blobs': len(objects[GitType.BLOB]),
                          'swh_num_trees': len(objects[GitType.TREE]),
                          'swh_num_commits': len(objects[GitType.COMM]),
                          'swh_num_releases': len(objects[GitType.RELE]),
                          'swh_id': log_id,
                      })

        return objects, objects_per_path

    def load_dir(self, dir_path, objects, objects_per_path, refs, origin_id):
        if self.config['send_contents']:
            self.bulk_send_blobs(objects_per_path, objects[GitType.BLOB],
                                 origin_id)
        else:
            self.log.info('Not sending contents')

        if self.config['send_directories']:
            self.bulk_send_trees(objects_per_path, objects[GitType.TREE])
        else:
            self.log.info('Not sending directories')

        if self.config['send_revisions']:
            self.bulk_send_commits(objects_per_path, objects[GitType.COMM])
        else:
            self.log.info('Not sending revisions')

        if self.config['send_releases']:
            self.bulk_send_annotated_tags(objects_per_path,
                                          objects[GitType.RELE])
        else:
            self.log.info('Not sending releases')

        if self.config['send_occurrences']:
            self.bulk_send_refs(objects_per_path, refs)
        else:
            self.log.info('Not sending occurrences')

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

        latest_revision = repo_metadata['entry_revision']
        print(latest_revision)

        for rev, parsed_objects in svn_tree_at(repo, latest_revision):
            print('rev: %s, ...' % rev)

        # def _occurrence_from(origin_id, revision_hash, occurrence):
        #     occ = dict(occurrence)
        #     occ.update({
        #         'target': revision_hash,
        #         'target_type': 'revision',
        #         'origin': origin_id,
        #     })
        #     return occ

        # def _occurrences_from(origin_id, revision_hash, occurrences):
        #     full_occs = []
        #     for occurrence in occurrences:
        #         full_occ = _occurrence_from(origin_id,
        #                                     revision_hash,
        #                                     occurrence)
        #         full_occs.append(full_occ)
        #     return full_occs


        # # to load the repository, walk all objects, compute their hash
        # objects, objects_per_path = self.list_repo_objs(dir_path, revision,
        #                                                 release)

        # full_rev = objects[GitType.COMM][0]  # only 1 revision

        # full_occs = _occurrences_from(origin['id'],
        #                               full_rev['id'],
        #                               occurrences)

        # self.load_dir(dir_path, objects, objects_per_path, full_occs,
        #               origin['id'])

        # objects[GitType.REFS] = full_occs

        # return {'status': True, 'objects': objects}


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
