# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing svn mirrors to
swh-storage.

"""

import abc
import os
import shutil

from swh.core import utils
from swh.model import hashutil
from swh.model.from_disk import Directory
from swh.model.identifiers import identifier_to_bytes, revision_identifier
from swh.model.identifiers import snapshot_identifier
from swh.loader.core.loader import SWHLoader

from . import svn, converters
from .config import DEFAULT_BRANCH
from .utils import init_svn_repo_from_archive_dump


def _revision_id(revision):
    return identifier_to_bytes(revision_identifier(revision))


def build_swh_snapshot(revision_id, origin_id, visit):
    """Build a swh snapshot from the revision id, origin id, and visit.

    """
    return {
        'id': None,
        'branches': {
            DEFAULT_BRANCH: {
                'target': revision_id,
                'target_type': 'revision',
                'origin': origin_id,
                'visit': visit,
            }
        }
    }


class SvnLoaderEventful(ValueError):
    """A wrapper exception to transit the swh_revision onto which the
    loading failed.

    """
    def __init__(self, e, swh_revision):
        super().__init__(e)
        self.swh_revision = swh_revision


class SvnLoaderUneventful(ValueError):
    def __init__(self, e, *args):
        super().__init__(e, *args)


class SvnLoaderHistoryAltered(ValueError):
    def __init__(self, e, *args):
        super().__init__(e, *args)


class BaseSvnLoader(SWHLoader, metaclass=abc.ABCMeta):
    """Base Svn loader to load one svn repository according to specific
    policies (only swh one now).

    The main entry point of this is (no need to override it)::

        def load(self, origin_visit, last_known_swh_revision=None): pass

    Inherit this class and then override the following functions::

        def build_swh_revision(self, rev, commit, dir_id, parents):
            # This is in charge of converting an svn revision to a compliant
            # swh revision

        def process_repository(self):
            # This is in charge of processing the actual svn repository and
            # store the result to swh storage.

    """
    CONFIG_BASE_FILENAME = 'loader/svn'

    ADDITIONAL_CONFIG = {
        'check_revision': ('int', 1000),
    }

    def __init__(self):
        super().__init__(logging_class='swh.loader.svn.SvnLoader')
        self.check_revision = self.config['check_revision']
        self.origin_id = None

    @abc.abstractmethod
    def swh_revision_hash_tree_at_svn_revision(self, revision):
        """Compute and return the hash tree at a given svn revision.

        Args:
            rev (int): the svn revision we want to check

        Returns:
            The hash tree directory as bytes.

        """
        pass

    @abc.abstractmethod
    def get_svn_repo(self, svn_url, destination_path, origin):
        """Instantiates the needed svnrepo collaborator to permit reading svn
        repository.

        Args:
            svn_url: the svn repository url to read from
            destination_path: the local path on disk to compute data
            origin: the corresponding origin

        Returns:
            Instance of :mod:`swh.loader.svn.svn` clients
        """
        raise NotImplementedError

    @abc.abstractmethod
    def build_swh_revision(self, rev, commit, dir_id, parents):
        """Convert an svn revision to an swh one according to the loader's
        policy (git-svn or swh).

        Args:
            rev: the svn revision number
            commit: dictionary with keys: author\_name, author\_date, rev,
                message
            dir_id: the hash tree computation
            parents: the revision's parents

        Returns:
            The swh revision
        """
        raise NotImplementedError

    @abc.abstractmethod
    def process_repository(self, origin_visit,
                           last_known_swh_revision=None,
                           start_from_scratch=False):
        """The main idea of this function is to:

        - iterate over the svn commit logs
        - extract the svn commit log metadata
        - compute the hashes from the current directory down to the file
        - compute the equivalent swh revision
        - send all those objects for storage
        - create an swh occurrence pointing to the last swh revision seen
        - send that occurrence for storage in swh-storage.

        """
        raise NotImplementedError

    def process_svn_revisions(self, svnrepo, revision_start, revision_end,
                              revision_parents):
        """Process revisions from revision_start to revision_end and send to
        swh for storage.

        At each svn revision, checkout the repository, compute the
        tree hash and blobs and send for swh storage to store.
        Then computes and yields the swh revision.

        Note that at every self.check_revision, an svn export is done
        and a hash tree is computed to check that no divergence
        occurred.

        Yields:
            swh revision as a dictionary with keys, sha1_git, sha1, etc...

        """
        gen_revs = svnrepo.swh_hash_data_per_revision(
            revision_start,
            revision_end)
        swh_revision = None
        count = 0
        for rev, nextrev, commit, new_objects, root_directory in gen_revs:
            count += 1
            # Send the associated contents/directories
            self.maybe_load_contents(new_objects.get('content', {}).values())
            self.maybe_load_directories(
                new_objects.get('directory', {}).values())

            # compute the fs tree's checksums
            dir_id = root_directory.hash
            swh_revision = self.build_swh_revision(
                rev, commit, dir_id, revision_parents[rev])

            swh_revision['id'] = _revision_id(swh_revision)

            self.log.debug('rev: %s, swhrev: %s, dir: %s' % (
                rev,
                hashutil.hash_to_hex(swh_revision['id']),
                hashutil.hash_to_hex(dir_id)))

            if (count % self.check_revision) == 0:  # hash computation check
                self.log.info('Checking hash computations on revision %s...' %
                              rev)
                checked_dir_id = self.swh_revision_hash_tree_at_svn_revision(
                    rev)
                if checked_dir_id != dir_id:
                    err = 'Hash tree computation divergence detected (%s != %s), stopping!' % (  # noqa
                        hashutil.hash_to_hex(dir_id),
                        hashutil.hash_to_hex(checked_dir_id)
                    )
                    raise ValueError(err)

            if nextrev:
                revision_parents[nextrev] = [swh_revision['id']]

            yield swh_revision

    def process_swh_revisions(self,
                              svnrepo,
                              revision_start,
                              revision_end,
                              revision_parents):
        """Process and store revision to swh (sent by blocks of
        revision_packet_size)

        Returns:
            The latest revision stored.

        """
        try:
            swh_revision_gen = self.process_svn_revisions(svnrepo,
                                                          revision_start,
                                                          revision_end,
                                                          revision_parents)
            revs = []
            for revisions in utils.grouper(
                    swh_revision_gen,
                    self.config['revision_packet_size']):
                revs = list(revisions)

                self.log.info('Processed %s revisions: [%s, ...]' % (
                    len(revs), hashutil.hash_to_hex(revs[0]['id'])))
                self.maybe_load_revisions(revs)
        except Exception as e:
            if revs:
                # flush remaining revisions
                self.maybe_load_revisions(revs)
                # Take the last one as the last known revisions
                known_swh_rev = revs[-1]

                _id = known_swh_rev.get('id')
                if not _id:
                    _id = _revision_id(known_swh_rev)

                # Then notify something is wrong, and we stopped at that rev.
                raise SvnLoaderEventful(e, swh_revision={
                    'id': _id,
                })
            raise e

        return revs[-1]

    def process_swh_snapshot(self, revision, origin_visit):
        """Process and load the occurrence pointing to the latest revision.

        """
        snap = build_swh_snapshot(
            revision['id'], origin_visit['origin'], origin_visit['visit'])
        snap_id = snapshot_identifier(snap)
        snap['id'] = identifier_to_bytes(snap_id)
        self.log.debug('snapshot: %s, id: %s' % (snap, snap_id))
        snap['id'] = identifier_to_bytes(snap_id)
        self.maybe_load_snapshot(snap)

    def prepare(self, *args, **kwargs):
        self.args = args

        destination_path = kwargs['destination_path']
        # local svn url
        svn_url = kwargs['svn_url']
        origin_url = kwargs.get('origin_url')
        self.visit_date = kwargs.get('visit_date')
        self.start_from_scratch = kwargs.get('start_from_scratch', False)

        origin = {
            'url': origin_url if origin_url else svn_url,
            'type': 'svn',
        }

        self.origin_id = self.send_origin(origin)
        origin['id'] = self.origin_id
        self.origin = origin

        if 'swh_revision' in kwargs:
            self.last_known_swh_revision = hashutil.hash_to_bytes(
                kwargs['swh_revision'])
        else:
            self.last_known_swh_revision = None

        self.svnrepo = self.get_svn_repo(svn_url, destination_path, origin)

    def get_origin(self):
        """Retrieve the origin we are working with (setup-ed in the prepare
           method)

        """
        return self.origin  # set in prepare method

    def fetch_data(self):
        """We need to fetch and stream the data to store directly.  So
        fetch_data do actually nothing.  The method ``store_data`` below is in
        charge to do everything, fetch and store.

        """
        pass

    def store_data(self):
        """We need to fetch and stream the data to store directly because
        there is too much data and state changes. Everything is
        intertwined together (We receive patch and apply on disk and
        compute at the hashes at the same time)

        So every data to fetch and store is done here.

        Note:
            origin_visit and last_known_swh_revision must have been set in the
            prepare method.

        """
        origin_visit = {'origin': self.origin_id, 'visit': self.visit}
        try:
            latest_rev = self.process_repository(
                origin_visit,
                last_known_swh_revision=self.last_known_swh_revision,
                start_from_scratch=self.start_from_scratch)
        except SvnLoaderUneventful as e:
            # Nothing needed to be done, the visit is full nonetheless
            self.log.info('Uneventful visit. Detail: %s' % e)
        except SvnLoaderEventful as e:
            self.log.error('Eventful partial visit. Detail: %s' % e)
            latest_rev = e.swh_revision
            self.process_swh_snapshot(latest_rev, origin_visit)
            raise
        except SvnLoaderHistoryAltered as e:
            self.log.error('History altered. Detail: %s' % e)
            raise
        else:
            self.process_swh_snapshot(latest_rev, origin_visit)


class SWHSvnLoader(BaseSvnLoader):
    """Swh svn loader is the main implementation destined for production.

    This implementation is able to deal with update on known svn repository.

    Default policy:
        Keep data as close as possible from the original svn data.  We
        only add information that are needed for update or continuing
        from last known revision (svn revision and svn repository's
        uuid).

    """
    def cleanup(self):
        """Clean after oneself.

        This is in charge to flush the remaining data to write in swh storage.
        And to clean up the svn repository's working representation on disk.
        """
        self.svnrepo.clean_fs()

    def swh_revision_hash_tree_at_svn_revision(self, revision):
        """Compute a given hash tree at specific revision.

        """
        local_dirname, local_url = self.svnrepo.export_temporary(revision)
        h = Directory.from_disk(path=local_url).hash
        self.svnrepo.clean_fs(local_dirname)
        return h

    def get_svn_repo(self, svn_url, destination_path, origin):
        return svn.SWHSvnRepo(
            svn_url, origin['id'], self.storage,
            destination_path=destination_path)

    def swh_previous_revision(self, prev_swh_revision=None):
        """Retrieve swh's previous revision if any.

        """
        self.log.debug('#####: %s' % prev_swh_revision)
        return self.svnrepo.swh_previous_revision(prev_swh_revision)

    def check_history_not_altered(self, svnrepo, revision_start, swh_rev):
        """Given a svn repository, check if the history was not tampered with.

        """
        revision_id = swh_rev['id']
        parents = swh_rev['parents']
        hash_data_per_revs = svnrepo.swh_hash_data_at_revision(revision_start)

        rev = revision_start
        rev, _, commit, _, root_dir = list(hash_data_per_revs)[0]

        dir_id = root_dir.hash
        swh_revision = self.build_swh_revision(rev,
                                               commit,
                                               dir_id,
                                               parents)
        swh_revision_id = _revision_id(swh_revision)

        return swh_revision_id == revision_id

    def build_swh_revision(self, rev, commit, dir_id, parents):
        """Build the swh revision dictionary.

        This adds:

        - the `'synthetic`' flag to true
        - the '`extra_headers`' containing the repository's uuid and the
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

    def init_from(self, partial_swh_revision, previous_swh_revision):
        """Function to determine from where to start from.

        Args:
            partial_swh_revision: A known revision from which
                the previous loading did not finish.
            known_previous_revision: A known revision from which the
                previous loading did finish.

        Returns:
            The revision from which to start or None if nothing (fresh
            start).

        """
        if partial_swh_revision and not previous_swh_revision:
            return partial_swh_revision
        if not partial_swh_revision and previous_swh_revision:
            return previous_swh_revision
        if partial_swh_revision and previous_swh_revision:
            # will determine from which to start from
            extra_headers1 = dict(
                partial_swh_revision['metadata']['extra_headers'])
            extra_headers2 = dict(
                previous_swh_revision['metadata']['extra_headers'])
            rev_start1 = int(extra_headers1['svn_revision'])
            rev_start2 = int(extra_headers2['svn_revision'])
            if rev_start1 <= rev_start2:
                return previous_swh_revision
            return partial_swh_revision

        return None

    def process_repository(self, origin_visit, last_known_swh_revision=None,
                           start_from_scratch=False):
        svnrepo = self.svnrepo

        # default configuration
        revision_start = 1
        revision_parents = {
            revision_start: []
        }

        if not start_from_scratch:
            # Check if we already know a previous revision for that origin
            swh_rev = self.swh_previous_revision()
            # Determine from which known revision to start
            swh_rev = self.init_from(last_known_swh_revision,
                                     previous_swh_revision=swh_rev)

            if swh_rev:  # Yes, we know a previous revision. Try and update it.
                extra_headers = dict(swh_rev['metadata']['extra_headers'])
                revision_start = int(extra_headers['svn_revision'])
                revision_parents = {
                    revision_start: swh_rev['parents'],
                }

                self.log.debug('svn export --ignore-keywords %s@%s' % (
                    svnrepo.remote_url,
                    revision_start))

                if swh_rev and not self.check_history_not_altered(
                        svnrepo,
                        revision_start,
                        swh_rev):
                    msg = 'History of svn %s@%s history modified. Skipping...' % (  # noqa
                        svnrepo.remote_url, revision_start)
                    raise SvnLoaderHistoryAltered(msg, *self.args)

                # now we know history is ok, we start at next revision
                revision_start = revision_start + 1
                # and the parent become the latest know revision for
                # that repository
                revision_parents[revision_start] = [swh_rev['id']]

        revision_end = svnrepo.head_revision()

        self.log.info('[revision_start-revision_end]: [%s-%s]' % (
            revision_start, revision_end))

        if revision_start > revision_end and revision_start is not 1:
            msg = '%s@%s already injected.' % (svnrepo.remote_url,
                                               revision_end)
            raise SvnLoaderUneventful(msg, *self.args)

        self.log.info('Processing %s.' % svnrepo)

        # process and store revision to swh (sent by by blocks of
        # 'revision_packet_size')
        return self.process_swh_revisions(
            svnrepo, revision_start, revision_end, revision_parents)


class SWHSvnLoaderFromDumpArchive(SWHSvnLoader):
    """Load a svn repository from an archive (containing a dump).

    """
    def __init__(self, archive_path):
        super().__init__()
        self.log.info('Archive to mount and load %s' % archive_path)
        self.temp_dir, self.repo_path = init_svn_repo_from_archive_dump(
            archive_path)

    def cleanup(self):
        super().cleanup()

        if self.temp_dir and os.path.exists(self.temp_dir):
            self.log.debug('Clean up temp directory %s for project %s' % (
                self.temp_dir, os.path.basename(self.repo_path)))
            shutil.rmtree(self.temp_dir)
