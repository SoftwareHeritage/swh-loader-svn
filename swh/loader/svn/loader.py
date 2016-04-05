# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.core import utils
from swh.model import git, hashutil
from swh.model.git import GitType

from swh.loader.svn import libloader, svn, converters


def objects_per_type(objects_per_path):
    """Given an object dictionary returned by
    `swh.model.git.walk_and_compute_sha1_from_directory`, return a map
    grouped by type.

    Returns:
        Dictionary with keys:
        - GitType.BLOB: list of blobs
        - GitType.TREE: list of directories

    """
    objects = {
        GitType.BLOB: [],
        GitType.TREE: [],
    }
    for tree_path in objects_per_path:
        objs = objects_per_path[tree_path]
        for obj in objs:
            objects[obj['type']].append(obj)

    return objects


class SvnLoader(libloader.SWHLoader):
    """Svn loader to load one svn repository.

    """

    def __init__(self, config, origin_id):
        super().__init__(config,
                         revision_type='svn',
                         origin_id=origin_id,
                         logging_class='swh.loader.svn.SvnLoader')

    def check_history_not_altered(self, svnrepo, revision_start, swh_rev):
        """Given a svn repository, check if the history was not tampered with.

        """
        revision_id = swh_rev['id']
        parents = swh_rev['parents']
        hash_data_per_revs = svnrepo.swh_hash_data_per_revision(revision_start,
                                                                revision_start)
        rev, _, commit, objects_per_path = list(hash_data_per_revs)[0]

        dir_id = objects_per_path[git.ROOT_TREE_KEY][0]['sha1_git']
        swh_revision = converters.build_swh_revision(svnrepo.uuid,
                                                     commit,
                                                     rev,
                                                     dir_id,
                                                     parents)
        swh_revision_id = git.compute_revision_sha1_git(swh_revision)

        return swh_revision_id == revision_id

    def process_svn_revisions(self, svnrepo, revision_start, revision_end,
                              revision_parents):
        """Process revisions from revision_start to revision_end and send to swh for
        storage.

        At each svn revision, checkout the repository, compute the
        tree hash and blobs and send for swh storage to store.
        Then computes and yields the swh revision.

        Yields:
            swh revision

        """
        gen_revs = svnrepo.swh_hash_data_per_revision(revision_start,
                                                      revision_end)
        for rev, nextrev, commit, objects_per_path in gen_revs:
            # compute the fs tree's checksums
            dir_id = objects_per_path[git.ROOT_TREE_KEY][0]['sha1_git']
            swh_revision = converters.build_swh_revision(svnrepo.uuid,
                                                         commit,
                                                         rev,
                                                         dir_id,
                                                         revision_parents[rev])
            swh_revision['id'] = git.compute_revision_sha1_git(swh_revision)
            self.log.debug('rev: %s, swhrev: %s' % (
                rev, hashutil.hash_to_hex(swh_revision['id'])))

            if nextrev:
                revision_parents[nextrev] = [swh_revision['id']]

            objects = objects_per_type(objects_per_path)

            self.maybe_load_contents(objects[GitType.BLOB])
            self.maybe_load_directories(objects[GitType.TREE],
                                        objects_per_path)

            yield swh_revision

    def process_swh_revisions(self,
                              svnrepo,
                              revision_start,
                              revision_end,
                              revision_parents):
        """Process and store revision to swh (sent by by blocks of
           'revision_packet_size')

           Returns:
                The latest revision stored.
        """
        for revisions in utils.grouper(
                self.process_svn_revisions(svnrepo,
                                           revision_start,
                                           revision_end,
                                           revision_parents),
                self.config['revision_packet_size']):
            revs = list(revisions)
            self.maybe_load_revisions(revs)

        return revs[-1]

    def process_swh_occurrence(self, revision, origin):
        """Process and load the occurrence pointing to the latest revision.

        """
        occ = converters.build_swh_occurrence(revision['id'],
                                              origin['id'],
                                              datetime.datetime.utcnow())
        self.log.debug('occ: %s' % occ)
        self.maybe_load_occurrences([occ])

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
        svnrepo = svn.SvnRepo(svn_url, origin['id'], self.storage,
                              destination_path)

        try:
            swh_rev = svnrepo.swh_previous_revision()

            if swh_rev:
                extra_headers = dict(swh_rev['metadata']['extra_headers'])
                revision_start = extra_headers['svn_revision']
                revision_parents = {
                    revision_start: swh_rev['parents']
                }
            else:
                revision_start = 1
                revision_parents = {
                    revision_start: []
                }

            svnrepo.fork(revision_start)
            self.log.debug('svn co %s@%s' % (svn_url, revision_start))

            if swh_rev and not self.check_history_not_altered(svnrepo,
                                                              revision_start,
                                                              swh_rev):
                msg = 'History of svn %s@%s history modified. Skipping...' % (
                    svn_url, revision_start)
                self.log.warn(msg)
                return {'status': False, 'stderr': msg}

            revision_end = svnrepo.head_revision()

            self.log.debug('[revision_start-revision_end]: [%s-%s]' % (
                revision_start, revision_end))

            if revision_start == revision_end and revision_start is not 1:
                self.log.info('%s@%s already injected.' % (svn_url,
                                                           revision_end))
                return {'status': True}

            self.log.info('Repo %s ready to be processed.' % svnrepo)

            # process and store revision to swh (sent by by blocks of
            # 'revision_packet_size')
            latest_rev = self.process_swh_revisions(svnrepo,
                                                    revision_start,
                                                    revision_end,
                                                    revision_parents)
            self.process_swh_occurrence(latest_rev, origin)

            # flush eventual remaining data
            self.flush()
        finally:
            svnrepo.clean_fs()

        return {'status': True}
