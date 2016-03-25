# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.core import hashutil
from swh.model import git
from swh.model.git import GitType

from swh.loader.svn import libloader, svn, converters


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
        svnrepo = svn.SvnRepo(svn_url, origin['id'], self.storage,
                              destination_path)
        self.log.debug('svnrepo: %s' % svnrepo)

        revision_start, revision_parents = svnrepo.swh_previous_revision_and_parents()  # noqa

        svnrepo.fork()

        self.log.debug('checkout at r1: %s' % svnrepo)

        revision_end = svnrepo.head_revision()

        self.log.debug('revision_end: %s' % revision_end)

        if not revision_start:
            revision_start = 1  # svnrepo.initial_revision()

        self.log.debug('initial revision: %s' % revision_start)

        if revision_start == revision_end and revision_start is not 1:
            self.log.info('%s@%s already injected.' % (svn_url, revision_end))
            return {'status': True}

        if revision_start == 1:
            parents = {revision_start: []}  # no parents for initial revision
        else:
            parents = {revision_start: revision_parents}

        swh_revisions = []
        # for each revision
        for rev, nextrev, commit, objects_per_path in svnrepo.swh_hash_data_per_revision(  # noqa
                revision_start, revision_end):
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
            swh_revision = converters.build_swh_revision(svnrepo.uuid,
                                                         commit,
                                                         rev,
                                                         dir_id,
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
        occ = converters.build_swh_occurrence(swh_revision['id'], origin['id'],
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

        # try:
        result = super().process(svn_url, origin, destination_path)
        # except:
        #     e_info = sys.exc_info()
        #     self.log.error('Problem during svn load for repo %s - %s' % (
        #         svn_url, e_info[1]))
        #     result = {'status': False, 'stderr': 'reason:%s\ntrace:%s' % (
        #             e_info[1],
        #             ''.join(traceback.format_tb(e_info[2])))}

        self.close_fetch_history(fetch_history_id, result)
