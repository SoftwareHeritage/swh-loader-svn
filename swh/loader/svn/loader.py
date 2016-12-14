# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing svn mirrors to
swh-storage.

"""

import abc
import datetime

from swh.core import utils, hashutil
from swh.model import git
from swh.model.git import GitType

from swh.loader.core.loader import SWHLoader
from . import svn, converters
from .utils import hashtree


class SvnLoaderEventful(ValueError):
    """A wrapper exception to transit the swh_revision onto which the
    loading failed.

    """
    def __init__(self, e, swh_revision):
        super().__init__(e)
        self.swh_revision = swh_revision


class SvnLoaderUneventful(ValueError):
    pass


class SvnLoaderHistoryAltered(ValueError):
    pass


class BaseSvnLoader(SWHLoader, metaclass=abc.ABCMeta):
    """Base Svn loader to load one svn repository according to specific
    policies (only swh one now).

    The main entry point of this is (no need to override it)
    - def load(self, origin_visit, last_known_swh_revision=None): pass

    Inherit this class and then override the following functions:
    - def build_swh_revision(self, rev, commit, dir_id, parents)
        This is in charge of converting an svn revision to a compliant
        swh revision

    - def process_repository(self)
        This is in charge of processing the actual svn repository and
        store the result to swh storage.

    """
    CONFIG_BASE_FILENAME = 'loader/svn'

    ADDITIONAL_CONFIG = {
        'check_revision': ('int', 1000),
    }

    def __init__(self):
        super().__init__(logging_class='swh.loader.svn.SvnLoader')
        self.check_revision = self.config['check_revision']

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
            Instance of swh.loader.svn.svn clients
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
    def process_repository(self, origin_visit, last_known_swh_revision=None):
        """The main idea of this function is to:
        - iterate over the svn commit logs
        - extract the svn commit log metadata
        - compute the hashes from the current directory down to the
          file
        - compute the equivalent swh revision
        - send all those objects for storage
        - create an swh occurrence pointing to the last swh revision
          seen
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
        for rev, nextrev, commit, objects_per_path in gen_revs:
            count += 1
            # Send the associated contents/directories
            self.maybe_load_contents(
                git.objects_per_type(GitType.BLOB, objects_per_path))
            self.maybe_load_directories(
                git.objects_per_type(GitType.TREE, objects_per_path))

            # compute the fs tree's checksums
            dir_id = objects_per_path[b'']['checksums']['sha1_git']
            swh_revision = self.build_swh_revision(
                rev, commit, dir_id, revision_parents[rev])

            swh_revision['id'] = git.compute_revision_sha1_git(
                swh_revision)

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
           'revision_packet_size')

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
                # Then notify something is wrong, and we stopped at that rev.
                raise SvnLoaderEventful(e, swh_revision={
                    'id': known_swh_rev['id'],
                })
            raise e

        return revs[-1]

    def process_swh_occurrence(self, revision, origin_visit):
        """Process and load the occurrence pointing to the latest revision.

        """
        occ = converters.build_swh_occurrence(revision['id'],
                                              origin_visit['origin'],
                                              origin_visit['visit'])
        self.log.debug('occ: %s' % occ)
        self.maybe_load_occurrences([occ])

    def close_success(self):
        self.close_fetch_history_success(self.fetch_history_id)
        self.storage.origin_visit_update(self.origin_visit['origin'],
                                         self.origin_visit['visit'],
                                         status='full')

    def close_failure(self):
        self.close_fetch_history_failure(self.fetch_history_id)
        self.storage.origin_visit_update(self.origin_visit['origin'],
                                         self.origin_visit['visit'],
                                         status='partial')

    def load(self, *args, **kwargs):
        """Load a svn repository in swh.

        Checkout the svn repository locally in destination_path.

        Args:
            - origin_visit: (mandatory) The current origin visit
            - last_known_swh_revision: (Optional) Hash id of known swh revision
              already visited in a previous visit

        Returns:
            Dictionary with the following keys:
            - eventful: (mandatory) is the loading being eventful or not
            - completion: (mandatory) 'full' if complete, 'partial' otherwise
            - state: (optional) if the completion was partial, this
              gives the state to pass along for the next schedule

        """
        self.prepare(*args, **kwargs)
        try:
            latest_rev = self.process_repository(
                self.origin_visit, self.last_known_swh_revision)
        except SvnLoaderEventful as e:
            self.log.error('Eventful partial visit. Detail: %s' % e)
            latest_rev = e.swh_revision
            self.process_swh_occurrence(latest_rev, self.origin_visit)
            self.close_failure()
            return {
                'eventful': True,
                'completion': 'partial',
                'state': {
                    'origin': self.origin_visit['origin'],
                    'revision': hashutil.hash_to_hex(latest_rev['id'])
                }
            }
        except (SvnLoaderHistoryAltered, SvnLoaderUneventful) as e:
            self.log.error('Uneventful visit. Detail: %s' % e)
            # FIXME: This fails because latest_rev is not bound
            # self.process_swh_occurrence(latest_rev, self.origin_visit)
            self.close_failure()
            return {
                'eventful': False,
            }
        except Exception as e:
            self.close_failure()
            raise e
        else:
            self.process_swh_occurrence(latest_rev, self.origin_visit)
            self.close_success()
            return {
                'eventful': True,
                'completion': 'full',
            }
        finally:
            self.clean()

    @abc.abstractmethod
    def clean(self):
        """Clean up after working.

        """
        pass

    def prepare(self, *args, **kwargs):
        """
        Prepare origin, fetch_origin, origin_visit
        Then load a svn repository.

        Then close origin_visit, fetch_history according to status success or
        failure.

        First:
        - creates an origin if it does not exist
        - creates a fetch_history entry
        - creates an origin_visit
        - Then loads the svn repository

        """
        destination_path = kwargs['destination_path']
        # local svn url
        svn_url = kwargs['svn_url']

        if 'origin' not in kwargs:  # first time, we'll create the origin
            origin = {
                'url': svn_url,
                'type': 'svn',
            }
            origin['id'] = self.storage.origin_add_one(origin)
        else:
            origin = {
                'id': kwargs['origin'],
                'url': svn_url,
                'type': 'svn'
            }

        if 'swh_revision' in kwargs:
            self.last_known_swh_revision = hashutil.hex_to_hash(
                kwargs['swh_revision'])
        else:
            self.last_known_swh_revision = None

        self.svnrepo = self.get_svn_repo(svn_url, destination_path, origin)
        self.origin_id = origin['id']

        self.fetch_history_id = self.open_fetch_history()

        date_visit = datetime.datetime.now(tz=datetime.timezone.utc)
        self.origin_visit = self.storage.origin_visit_add(
            self.origin_id, date_visit)


class SWHSvnLoader(BaseSvnLoader):
    """Swh svn loader is the main implementation destined for production.
    This implementation is able to deal with update on known svn repository.

    Default policy:
        It's to not add any information and be as close as possible
        from the svn data the server sent its way.

        The only thing that are added are the swh's revision
        'extra_header' to be able to deal with update.

    """
    def __init__(self):
        super().__init__()

    def clean(self):
        """Clean after oneself.

        This is in charge to flush the remaining data to write in swh storage.
        And to clean up the svn repository's working representation on disk.
        """
        self.flush()
        self.svnrepo.clean_fs()

    def swh_revision_hash_tree_at_svn_revision(self, revision):
        """Compute a given hash tree at specific revision.

        """
        local_url = self.svnrepo.export_temporary(revision)
        h = hashtree(local_url)['sha1_git']
        self.svnrepo.clean_fs(local_url)
        return h

    def get_svn_repo(self, svn_url, destination_path, origin):
        return svn.SWHSvnRepo(
            svn_url, origin['id'], self.storage,
            destination_path=destination_path)

    def swh_previous_revision(self, prev_swh_revision=None):
        """Retrieve swh's previous revision if any.

        """
        return self.svnrepo.swh_previous_revision(prev_swh_revision)

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

    def init_from(self, partial_swh_revision, previous_swh_revision):
        """Function to determine from where to start from.

        Args:
            - partial_swh_revision: A known revision from which
            the previous loading did not finish.
            - known_previous_revision: A known revision from which the
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

    def process_repository(self, origin_visit, last_known_swh_revision=None):
        svnrepo = self.svnrepo

        # default configuration
        revision_start = 1
        revision_parents = {
            revision_start: []
        }

        # Check if we already know a previous revision for that origin
        swh_rev = self.swh_previous_revision()
        # Determine from which known revision to start
        swh_rev = self.init_from(last_known_swh_revision,
                                 previous_swh_revision=swh_rev)

        if swh_rev:  # Yes, we do. Try and update it.
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
                self.log.warn(msg)
                raise SvnLoaderHistoryAltered
            else:
                # now we know history is ok, we start at next revision
                revision_start = revision_start + 1
                # and the parent become the latest know revision for
                # that repository
                revision_parents[revision_start] = [swh_rev['id']]

        revision_end = svnrepo.head_revision()

        self.log.info('[revision_start-revision_end]: [%s-%s]' % (
            revision_start, revision_end))

        if revision_start > revision_end and revision_start is not 1:
            self.log.info('%s@%s already injected.' % (
                svnrepo.remote_url, revision_end))
            raise SvnLoaderUneventful

        self.log.info('Processing %s.' % svnrepo)

        # process and store revision to swh (sent by by blocks of
        # 'revision_packet_size')
        return self.process_swh_revisions(
            svnrepo, revision_start, revision_end, revision_parents)
