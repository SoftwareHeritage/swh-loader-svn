# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing svn mirrors to
swh-storage.

"""

import datetime

from swh.core import utils
from swh.model import git, hashutil
from swh.model.git import GitType

from swh.loader.core.loader import SWHLoader
from . import svn, converters


class BaseSvnLoader(SWHLoader):
    """Base Svn loader to load one svn repository.

    There exists 2 different policies:
    - git-svn one (not for production): cf. GitSvnLoader
    - SWH one: cf. SWHSvnLoader

    """
    CONFIG_BASE_FILENAME = 'loader/svn.ini'

    def __init__(self, svn_url, destination_path, origin,
                 with_svn_update=True):
        super().__init__(origin['id'],
                         logging_class='swh.loader.svn.SvnLoader')
        self.with_svn_update = with_svn_update  # noqa
        self.origin = origin

    def build_swh_revision(self, rev, commit, dir_id, parents):
        """Convert an svn revision to an swh one according to the loader's
        policy (git-svn or swh).

        Args:
            rev: the svn revision number
            commit: dictionary with keys 'author_name', 'author_date', 'rev',
            'message'
            dir_id: the hash tree computation
            parents: the revision's parents

        Returns:
            The swh revision
        """
        raise NotImplementedError('This should be overriden by subclass')

    def check_history_not_altered(self, svnrepo, revision_start, swh_rev):
        """Given a svn repository, check if the history was not tampered with.

        """
        revision_id = swh_rev['id']
        parents = swh_rev['parents']
        hash_data_per_revs = svnrepo.swh_hash_data_at_revision(revision_start)

        rev = revision_start
        rev, _, commit, objects_per_path = list(hash_data_per_revs)[0]

        dir_id = objects_per_path[b'']['checksums']['sha1_git']
        swh_revision = self.build_swh_revision(rev,
                                               commit,
                                               dir_id,
                                               parents)
        swh_revision_id = git.compute_revision_sha1_git(swh_revision)

        return swh_revision_id == revision_id

    def process_svn_revisions(self, svnrepo, revision_start, revision_end,
                              revision_parents):
        """Process revisions from revision_start to revision_end and send to
        swh for storage.

        At each svn revision, checkout the repository, compute the
        tree hash and blobs and send for swh storage to store.
        Then computes and yields the swh revision.

        Yields:
            swh revision

        """
        gen_revs = svnrepo.swh_hash_data_per_revision(
            revision_start,
            revision_end)
        for rev, nextrev, commit, objects_per_path in gen_revs:
            # compute the fs tree's checksums
            dir_id = objects_per_path[b'']['checksums']['sha1_git']
            swh_revision = self.build_swh_revision(
                rev, commit, dir_id, revision_parents[rev])
            swh_revision['id'] = git.compute_revision_sha1_git(swh_revision)
            self.log.debug('rev: %s, swhrev: %s, dir: %s' % (
                rev,
                hashutil.hash_to_hex(swh_revision['id']),
                hashutil.hash_to_hex(dir_id)))

            if nextrev:
                revision_parents[nextrev] = [swh_revision['id']]

            self.maybe_load_contents(
                git.objects_per_type(GitType.BLOB, objects_per_path))
            self.maybe_load_directories(
                git.objects_per_type(GitType.TREE, objects_per_path))

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
            self.log.info('Processed %s revisions: [%s, ...]' % (
                len(revs), hashutil.hash_to_hex(revs[0]['id'])))
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

    def process(self):
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
        svnrepo = self.svnrepo
        origin = self.origin
        try:
            # default configuration
            revision_start = 1
            revision_parents = {
                revision_start: []
            }

            if self.with_svn_update:  # Do we want to deal with update?
                swh_rev = svnrepo.swh_previous_revision()

                if swh_rev:  # Yes, we do. Try and update it.
                    extra_headers = dict(swh_rev['metadata']['extra_headers'])
                    revision_start = int(extra_headers['svn_revision'])
                    revision_parents = {
                        revision_start: swh_rev['parents']
                    }

                    self.log.debug('svn co %s@%s' % (svnrepo.remote_url,
                                                     revision_start))

                    if swh_rev and not self.check_history_not_altered(
                            svnrepo,
                            revision_start,
                            swh_rev):
                        msg = 'History of svn %s@%s history modified. Skipping...' % (  # noqa
                            svnrepo.remote_url, revision_start)
                        self.log.warn(msg)
                        return {'status': False, 'stderr': msg}

            revision_end = svnrepo.head_revision()

            self.log.info('[revision_start-revision_end]: [%s-%s]' % (
                revision_start, revision_end))

            if revision_start == revision_end and revision_start is not 1:
                self.log.info('%s@%s already injected.' % (
                    svnrepo.remote_url, revision_end))
                return {'status': True}

            self.log.info('Processing %s.' % svnrepo)

            # process and store revision to swh (sent by by blocks of
            # 'revision_packet_size')
            latest_rev = self.process_swh_revisions(svnrepo,
                                                    revision_start,
                                                    revision_end,
                                                    revision_parents)
            self.process_swh_occurrence(latest_rev, origin)

        finally:
            # flush eventual remaining data
            self.flush()
            svnrepo.clean_fs()

        return {'status': True}


class GitSvnSvnLoader(BaseSvnLoader):
    """Git-svn like loader (compute hashes a-la git-svn)

    Notes:
        This implementation is:
        - NOT for production
        - NOT able to deal with update.

    Default policy:
        Its default policy is to enrich (or even alter) information at
        each svn revision. It will:

        - truncate the timestamp of the svn commit date
        - alter the user to be an email using the repository's uuid as
          mailserver (user -> user@<repo-uuid>)
        - fills in the gap for empty author with '(no author)' name
        - remove empty folder (thus not counting them during hash computation)

        The equivalent git command is: `git svn clone <repo-url> -q
        --no-metadata`

    """
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin,
                         with_svn_update=False)
        self.svnrepo = svn.GitSvnSvnRepo(
            svn_url,
            origin['id'],
            self.storage,
            destination_path=destination_path)

    def build_swh_revision(self, rev, commit, dir_id, parents):
        """Build the swh revision a-la git-svn.

        Args:
            rev: the svn revision
            commit: the commit metadata
            dir_id: the upper tree's hash identifier
            parents: the parents' identifiers

        Returns:
            The swh revision corresponding to the svn revision
            without any extra headers.

        """
        return converters.build_gitsvn_swh_revision(rev,
                                                    commit,
                                                    dir_id,
                                                    parents)


class SWHSvnLoader(BaseSvnLoader):
    """Swh svn loader is the main implementation destined for production.
    This implementation is able to deal with update on known svn repository.

    Default policy:
        It's to not add any information and be as close as possible
        from the svn data the server sent its way.

        The only thing that are added are the swh's revision
        'extra_header' to be able to deal with update.

    """
    def __init__(self, svn_url, destination_path, origin):
        super().__init__(svn_url, destination_path, origin)
        self.svnrepo = svn.SWHSvnRepo(
            svn_url,
            origin['id'],
            self.storage,
            destination_path=destination_path)

    def build_swh_revision(self, rev, commit, dir_id, parents):
        """Build the swh revision dictionary.

        This adds:
        - the 'synthetic' flag to true
        - the 'extra_headers' containing the repository's uuid and the
          svn revision number.

        Args:
            rev: the svn revision
            commit: the commit metadata
            dir_id: the upper tree's hash identifier
            parents: the parents' identifiers

        Returns:
            The swh revision corresponding to the svn revision.

        """
        return converters.build_swh_revision(rev,
                                             commit,
                                             self.svnrepo.uuid,
                                             dir_id,
                                             parents)
