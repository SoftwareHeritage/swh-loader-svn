# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing svn mirrors to
swh-storage.

"""

import os
import shutil
import tempfile

from swh.model import hashutil
from swh.model.from_disk import Directory
from swh.model.identifiers import identifier_to_bytes, revision_identifier
from swh.model.identifiers import snapshot_identifier
from swh.loader.core.loader import SWHLoader
from swh.loader.core.utils import clean_dangling_folders

from . import svn, converters
from .utils import init_svn_repo_from_archive_dump
from .exception import SvnLoaderUneventful
from .exception import SvnLoaderHistoryAltered


DEFAULT_BRANCH = b'master'


def _revision_id(revision):
    return identifier_to_bytes(revision_identifier(revision))


def build_swh_snapshot(revision_id, branch=DEFAULT_BRANCH):
    """Build a swh snapshot from the revision id, origin id, and visit.

    """
    return {
        'id': None,
        'branches': {
            branch: {
                'target': revision_id,
                'target_type': 'revision',
            }
        }
    }


TEMPORARY_DIR_PREFIX_PATTERN = 'swh.loader.svn.'


class SvnLoader(SWHLoader):
    """Swh svn loader to load an svn repository The repository is either
    remote or local.  The loader deals with update on an already
    previously loaded repository.

    Default policy:
        Keep data as close as possible from the original svn data.  We
        only add information that are needed for update or continuing
        from last known revision (svn revision and svn repository's
        uuid).

    """
    CONFIG_BASE_FILENAME = 'loader/svn'

    ADDITIONAL_CONFIG = {
        'check_revision': ('int', 1000),
        'temp_directory': ('str', '/tmp'),
        'debug': ('bool', False),  # NOT FOR PRODUCTION, False is mandatory
    }

    def __init__(self):
        super().__init__(logging_class='swh.loader.svn.SvnLoader')
        self.check_revision = self.config['check_revision']
        self.origin_id = None
        self.debug = self.config['debug']
        self.last_seen_revision = None
        self.temp_directory = self.config['temp_directory']
        self.done = False
        # internal state used to store swh objects
        self._contents = []
        self._directories = []
        self._revisions = []
        self._snapshot = None
        self._last_revision = None
        self._visit_status = 'full'
        self._load_status = 'uneventful'

    def pre_cleanup(self):
        """Cleanup potential dangling files from prior runs (e.g. OOM killed
           tasks)

        """
        clean_dangling_folders(self.temp_directory,
                               pattern_check=TEMPORARY_DIR_PREFIX_PATTERN,
                               log=self.log)

    def cleanup(self):
        """Clean up the svn repository's working representation on disk.

        """
        if not hasattr(self, 'svnrepo'):
            # could happen if `prepare` fails
            # nothing to do in that case
            return
        if self.debug:
            self.log.error('''NOT FOR PRODUCTION - debug flag activated
Local repository not cleaned up for investigation: %s''' % (
                self.svnrepo.local_url.decode('utf-8'), ))
            return
        self.svnrepo.clean_fs()

    def swh_revision_hash_tree_at_svn_revision(self, revision):
        """Compute and return the hash tree at a given svn revision.

        Args:
            rev (int): the svn revision we want to check

        Returns:
            The hash tree directory as bytes.

        """
        local_dirname, local_url = self.svnrepo.export_temporary(revision)
        h = Directory.from_disk(path=local_url).hash
        self.svnrepo.clean_fs(local_dirname)
        return h

    def get_svn_repo(self, svn_url, local_dirname, origin):
        """Instantiates the needed svnrepo collaborator to permit reading svn
        repository.

        Args:
            svn_url (str): the svn repository url to read from
            local_dirname (str): the local path on disk to compute data
            origin (int): the corresponding origin

        Returns:
            Instance of :mod:`swh.loader.svn.svn` clients
        """
        return svn.SvnRepo(
            svn_url, origin['id'], self.storage,
            local_dirname=local_dirname)

    def swh_latest_snapshot_revision(self, origin_id,
                                     previous_swh_revision=None):
        """Look for latest snapshot revision and returns it if any.

        Args:
            origin_id (int): Origin identifier
            previous_swh_revision: (optional) id of a possible
                                   previous swh revision

        Returns:
            dict: The latest known point in time. Dict with keys:

                'revision': latest visited revision
                'snapshot': latest snapshot

            If None is found, return an empty dict.

        """
        storage = self.storage
        if not previous_swh_revision:  # check latest snapshot's revision
            latest_snap = storage.snapshot_get_latest(origin_id)
            if latest_snap:
                branches = latest_snap.get('branches')
                if not branches:
                    return {}
                branch = branches.get(DEFAULT_BRANCH)
                if not branch:
                    return {}
                target_type = branch['target_type']
                if target_type != 'revision':
                    return {}
                previous_swh_revision = branch['target']
            else:
                return {}

        if isinstance(previous_swh_revision, dict):
            swh_id = previous_swh_revision['id']
        else:
            swh_id = previous_swh_revision

        revs = list(storage.revision_get([swh_id]))
        if revs:
            return {
                'snapshot': latest_snap,
                'revision': revs[0]
            }
        return {}

    def build_swh_revision(self, rev, commit, dir_id, parents):
        """Build the swh revision dictionary.

        This adds:

        - the `'synthetic`' flag to true
        - the '`extra_headers`' containing the repository's uuid and the
          svn revision number.

        Args:
            rev (dict): the svn revision
            commit (dict): the commit metadata
            dir_id (bytes): the upper tree's hash identifier
            parents ([bytes]): the parents' identifiers

        Returns:
            The swh revision corresponding to the svn revision.

        """
        return converters.build_swh_revision(rev,
                                             commit,
                                             self.svnrepo.uuid,
                                             dir_id,
                                             parents)

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

    def _init_from(self, partial_swh_revision, previous_swh_revision):
        """Function to determine from where to start from.

        Args:
            partial_swh_revision (dict): A known revision from which
                                         the previous loading did not
                                         finish.
            known_previous_revision (dict): A known revision from
                                            which the previous loading
                                            did finish.

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

    def start_from(self, last_known_swh_revision=None,
                   start_from_scratch=False):
        """Determine from where to start the loading.

        Args:
            last_known_swh_revision (dict): Last know swh revision or None
            start_from_scratch (bool): To start loading from scratch or not

        Returns:
            tuple (revision_start, revision_end, revision_parents)

        Raises:

            SvnLoaderHistoryAltered: When a hash divergence has been
                                     detected (should not happen)
            SvnLoaderUneventful: Nothing changed since last visit

        """
        revision_head = self.svnrepo.head_revision()
        if revision_head == 0:  # empty repository case
            revision_start = 0
            revision_end = 0
        else:  # default configuration
            revision_start = self.svnrepo.initial_revision()
            revision_end = revision_head

        revision_parents = {
            revision_start: []
        }

        if not start_from_scratch:
            # Check if we already know a previous revision for that origin
            if self.latest_snapshot:
                swh_rev = self.latest_snapshot['revision']
            else:
                swh_rev = None

            # Determine from which known revision to start
            swh_rev = self._init_from(last_known_swh_revision,
                                      previous_swh_revision=swh_rev)

            if swh_rev:  # Yes, we know a previous revision. Try and update it.
                extra_headers = dict(swh_rev['metadata']['extra_headers'])
                revision_start = int(extra_headers['svn_revision'])
                revision_parents = {
                    revision_start: swh_rev['parents'],
                }

                self.log.debug('svn export --ignore-keywords %s@%s' % (
                    self.svnrepo.remote_url,
                    revision_start))

                if swh_rev and not self.check_history_not_altered(
                        self.svnrepo,
                        revision_start,
                        swh_rev):
                    msg = 'History of svn %s@%s altered. ' \
                          'Skipping...' % (
                              self.svnrepo.remote_url, revision_start)
                    raise SvnLoaderHistoryAltered(msg)

                # now we know history is ok, we start at next revision
                revision_start = revision_start + 1
                # and the parent become the latest know revision for
                # that repository
                revision_parents[revision_start] = [swh_rev['id']]

        if revision_start > revision_end and revision_start is not 1:
            msg = '%s@%s already injected.' % (self.svnrepo.remote_url,
                                               revision_end)
            raise SvnLoaderUneventful(msg)

        self.log.info('Processing revisions [%s-%s] for %s' % (
            revision_start, revision_end, self.svnrepo))

        return revision_start, revision_end, revision_parents

    def process_svn_revisions(self, svnrepo, revision_start, revision_end,
                              revision_parents):
        """Process svn revisions from revision_start to revision_end.

        At each svn revision, checkout the repository, compute the
        tree hash and blobs and send for swh storage to store.  Then
        computes and yields the computed swh contents, directories,
        revision.

        Note that at every self.check_revision, an svn export is done
        and a hash tree is computed to check that no divergence
        occurred.

        Yields:
            tuple (contents, directories, revision) of dict as a
            dictionary with keys, sha1_git, sha1, etc...

        Raises:
            ValueError in case of a hash divergence detection

        """
        gen_revs = svnrepo.swh_hash_data_per_revision(
            revision_start,
            revision_end)
        swh_revision = None
        count = 0
        for rev, nextrev, commit, new_objects, root_directory in gen_revs:
            count += 1
            # Send the associated contents/directories
            _contents = new_objects.get('content', {}).values()
            _directories = new_objects.get('directory', {}).values()

            # compute the fs tree's checksums
            dir_id = root_directory.hash
            swh_revision = self.build_swh_revision(
                rev, commit, dir_id, revision_parents[rev])

            swh_revision['id'] = _revision_id(swh_revision)

            self.log.debug('rev: %s, swhrev: %s, dir: %s' % (
                rev,
                hashutil.hash_to_hex(swh_revision['id']),
                hashutil.hash_to_hex(dir_id)))

            # FIXME: Is that still necessary? Rationale: T570 is now closed
            if (count % self.check_revision) == 0:  # hash computation check
                self.log.debug('Checking hash computations on revision %s...' %
                               rev)
                checked_dir_id = self.swh_revision_hash_tree_at_svn_revision(
                    rev)
                if checked_dir_id != dir_id:
                    err = 'Hash tree computation divergence detected ' \
                          '(%s != %s), stopping!' % (
                              hashutil.hash_to_hex(dir_id),
                              hashutil.hash_to_hex(checked_dir_id))
                    raise ValueError(err)

            if nextrev:
                revision_parents[nextrev] = [swh_revision['id']]

            yield _contents, _directories, swh_revision

    def prepare_origin_visit(self, *, svn_url, visit_date=None,
                             origin_url=None, **kwargs):
        self.origin = {
            'url': origin_url if origin_url else svn_url,
            'type': 'svn',
        }
        self.visit_date = visit_date

    def prepare(self, *, svn_url, destination_path=None,
                swh_revision=None, start_from_scratch=False, **kwargs):
        self.start_from_scratch = start_from_scratch
        if swh_revision:
            self.last_known_swh_revision = swh_revision
        else:
            self.last_known_swh_revision = None

        self.latest_snapshot = self.swh_latest_snapshot_revision(
            self.origin_id, self.last_known_swh_revision)

        if destination_path:
            local_dirname = destination_path
        else:
            local_dirname = tempfile.mkdtemp(
                suffix='-%s' % os.getpid(),
                prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                dir=self.temp_directory)

        self.svnrepo = self.get_svn_repo(svn_url, local_dirname, self.origin)
        try:
            revision_start, revision_end, revision_parents = self.start_from(
                self.last_known_swh_revision, self.start_from_scratch)
            self.swh_revision_gen = self.process_svn_revisions(
                self.svnrepo, revision_start, revision_end, revision_parents)
        except SvnLoaderUneventful as e:
            self.log.warn(e)
            if self.latest_snapshot and 'snapshot' in self.latest_snapshot:
                self._snapshot = self.latest_snapshot['snapshot']
            self.done = True
        except SvnLoaderHistoryAltered as e:
            self.log.error(e)
            self.done = True
            self._visit_status = 'partial'

    def fetch_data(self):
        """Fetching svn revision information.

        This will apply svn revision as patch on disk, and at the same
        time, compute the swh hashes.

        In effect, fetch_data fetches those data and compute the
        necessary swh objects. It's then stored in the internal state
        instance variables (initialized in `_prepare_state`).

        This is up to `store_data` to actually discuss with the
        storage to store those objects.

        Returns:
            bool: True to continue fetching data (next svn revision),
            False to stop.

        """
        data = None
        if self.done:
            return False

        try:
            data = next(self.swh_revision_gen)
            self._load_status = 'eventful'
        except StopIteration:
            self.done = True
            self._visit_status = 'full'
            return False  # Stopping iteration
        except Exception as e:  # Potential: svn:external, i/o error...
            self.done = True
            self._visit_status = 'partial'
            return False  # Stopping iteration
        self._contents, self._directories, revision = data
        if revision:
            self._last_revision = revision
        self._revisions.append(revision)
        return True  # next svn revision

    def store_data(self):
        """We store the data accumulated in internal instance variable.  If
           the iteration over the svn revisions is done, we create the
           snapshot and flush to storage the data.

           This also resets the internal instance variable state.

        """
        self.maybe_load_contents(self._contents)
        self.maybe_load_directories(self._directories)
        self.maybe_load_revisions(self._revisions)

        if self.done:  # finish line, snapshot!
            self.generate_and_load_snapshot(revision=self._last_revision,
                                            snapshot=self._snapshot)
            self.flush()

        self._contents = []
        self._directories = []
        self._revisions = []

    def generate_and_load_snapshot(self, revision=None, snapshot=None):
        """Create the snapshot either from existing revision or snapshot.

        Revision (supposedly new) has priority over the snapshot
        (supposedly existing one).

        Args:
            revision (dict): Last revision seen if any (None by default)
            snapshot (dict): Snapshot to use if any (None by default)

        """
        if revision:  # Priority to the revision
            snap = build_swh_snapshot(revision['id'])
            snap['id'] = identifier_to_bytes(snapshot_identifier(snap))
        elif snapshot:  # Fallback to prior snapshot
            snap = snapshot
        else:
            return None
        self.log.debug('snapshot: %s' % snap)
        self.maybe_load_snapshot(snap)

    def load_status(self):
        return {
            'status': self._load_status,
        }

    def visit_status(self):
        return self._visit_status


class SvnLoaderFromDumpArchive(SvnLoader):
    """Uncompress an archive containing an svn dump, mount the svn dump as
       an svn repository and load said repository.

    """
    def __init__(self, archive_path):
        super().__init__()
        self.log.info('Archive to mount and load %s' % archive_path)
        self.temp_dir, self.repo_path = init_svn_repo_from_archive_dump(
            archive_path,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix='-%s' % os.getpid(),
            root_dir=self.temp_directory)

    def cleanup(self):
        super().cleanup()

        if self.temp_dir and os.path.exists(self.temp_dir):
            msg = 'Clean up temporary directory dump %s for project %s' % (
                self.temp_dir, os.path.basename(self.repo_path))
            self.log.debug(msg)
            shutil.rmtree(self.temp_dir)
