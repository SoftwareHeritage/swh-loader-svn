# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing svn mirrors to
swh-storage.

"""
from mmap import ACCESS_WRITE, mmap
import os
import pty
import re
import shutil
from subprocess import Popen
import tempfile
from typing import Any, Dict, Iterator, List, Optional, Tuple

from swh.core.config import merge_configs
from swh.loader.core.loader import BaseLoader
from swh.loader.core.utils import clean_dangling_folders
from swh.model import from_disk, hashutil
from swh.model.model import (
    Content,
    Directory,
    Origin,
    Revision,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.algos.snapshot import snapshot_get_latest

from . import converters, svn
from .exception import SvnLoaderHistoryAltered, SvnLoaderUneventful
from .utils import (
    OutputStream,
    init_svn_repo_from_archive_dump,
    init_svn_repo_from_dump,
)

DEFAULT_BRANCH = b"HEAD"


TEMPORARY_DIR_PREFIX_PATTERN = "swh.loader.svn."


DEFAULT_CONFIG: Dict[str, Any] = {
    "temp_directory": "/tmp",
    "debug": False,  # NOT FOR PRODUCTION: False for production
    "check_revision": {
        "status": False,  # True: check the revision, False: don't check
        "limit": 1000,  # Periodicity check
    },
}


class SvnLoader(BaseLoader):
    """Swh svn loader.

    The repository is either remote or local.  The loader deals with
    update on an already previously loaded repository.

    """

    visit_type = "svn"

    def __init__(
        self,
        url,
        origin_url=None,
        visit_date=None,
        destination_path=None,
        swh_revision=None,
        start_from_scratch=False,
    ):
        super().__init__(logging_class="swh.loader.svn.SvnLoader")
        self.config = merge_configs(DEFAULT_CONFIG, self.config)
        # technical svn uri to act on svn repository
        self.svn_url = url
        # origin url as unique identifier for origin in swh archive
        self.origin_url = origin_url if origin_url else self.svn_url
        self.debug = self.config["debug"]
        self.temp_directory = self.config["temp_directory"]
        self.done = False
        self.svnrepo = None
        # Revision check is configurable
        check_revision = self.config["check_revision"]
        if check_revision["status"]:
            self.check_revision = check_revision["limit"]
        else:
            self.check_revision = None
        # internal state used to store swh objects
        self._contents = []
        self._skipped_contents = []
        self._directories = []
        self._revisions = []
        self._snapshot: Optional[Snapshot] = None
        # internal state, current visit
        self._last_revision = None
        self._visit_status = "full"
        self._load_status = "uneventful"
        self.visit_date = visit_date
        self.destination_path = destination_path
        self.start_from_scratch = start_from_scratch
        self.max_content_length = self.config["max_content_size"]
        self.snapshot = None
        # state from previous visit
        self.latest_snapshot = None
        self.latest_revision = None

    def pre_cleanup(self):
        """Cleanup potential dangling files from prior runs (e.g. OOM killed
           tasks)

        """
        clean_dangling_folders(
            self.temp_directory,
            pattern_check=TEMPORARY_DIR_PREFIX_PATTERN,
            log=self.log,
        )

    def cleanup(self):
        """Clean up the svn repository's working representation on disk.

        """
        if not self.svnrepo:  # could happen if `prepare` fails
            return
        if self.debug:
            self.log.error(
                """NOT FOR PRODUCTION - debug flag activated
Local repository not cleaned up for investigation: %s"""
                % (self.svnrepo.local_url.decode("utf-8"),)
            )
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
        h = from_disk.Directory.from_disk(path=local_url).hash
        self.svnrepo.clean_fs(local_dirname)
        return h

    def _latest_snapshot_revision(
        self, origin_url: str,
    ) -> Optional[Tuple[Snapshot, Revision]]:
        """Look for latest snapshot revision and returns it if any.

        Args:
            origin_url: Origin identifier
            previous_swh_revision: possible previous swh revision (either a dict or
                revision identifier)

        Returns:
            Tuple of the latest Snapshot from the previous visit and its targeted
            revision if any or None otherwise.

        """
        storage = self.storage
        latest_snapshot = snapshot_get_latest(storage, origin_url)
        if not latest_snapshot:
            return None
        branches = latest_snapshot.branches
        if not branches:
            return None
        branch = branches.get(DEFAULT_BRANCH)
        if not branch:
            return None
        if branch.target_type != TargetType.REVISION:
            return None
        swh_id = branch.target

        revision = storage.revision_get([swh_id])[0]
        if not revision:
            return None
        return latest_snapshot, revision

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
        return converters.build_swh_revision(
            rev, commit, self.svnrepo.uuid, dir_id, parents
        )

    def check_history_not_altered(
        self, svnrepo, revision_start: int, swh_rev: Revision
    ) -> bool:
        """Given a svn repository, check if the history was modified in between visits.

        """
        revision_id = swh_rev.id
        parents = swh_rev.parents
        hash_data_per_revs = svnrepo.swh_hash_data_at_revision(revision_start)

        rev = revision_start
        rev, _, commit, _, root_dir = list(hash_data_per_revs)[0]

        dir_id = root_dir.hash
        swh_revision = self.build_swh_revision(rev, commit, dir_id, parents)
        swh_revision_id = swh_revision.id
        return swh_revision_id == revision_id

    def start_from(
        self, start_from_scratch: bool = False
    ) -> Tuple[int, int, Dict[int, Tuple[bytes, ...]]]:
        """Determine from where to start the loading.

        Args:
            start_from_scratch: As opposed to start from the last snapshot

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

        revision_parents: Dict[int, Tuple[bytes, ...]] = {revision_start: ()}

        # start from a previous revision if any
        if not start_from_scratch and self.latest_revision is not None:
            extra_headers = dict(self.latest_revision.extra_headers)
            revision_start = int(extra_headers[b"svn_revision"])
            revision_parents = {
                revision_start: self.latest_revision.parents,
            }

            self.log.debug(
                "svn export --ignore-keywords %s@%s"
                % (self.svnrepo.remote_url, revision_start)
            )

            if not self.check_history_not_altered(
                self.svnrepo, revision_start, self.latest_revision
            ):
                msg = "History of svn %s@%s altered. Skipping..." % (
                    self.svnrepo.remote_url,
                    revision_start,
                )
                raise SvnLoaderHistoryAltered(msg)

            # now we know history is ok, we start at next revision
            revision_start = revision_start + 1
            # and the parent become the latest know revision for
            # that repository
            revision_parents[revision_start] = (self.latest_revision.id,)

        if revision_start > revision_end and revision_start != 1:
            msg = "%s@%s already injected." % (self.svnrepo.remote_url, revision_end)
            raise SvnLoaderUneventful(msg)

        self.log.info(
            "Processing revisions [%s-%s] for %s"
            % (revision_start, revision_end, self.svnrepo)
        )

        return revision_start, revision_end, revision_parents

    def _check_revision_divergence(self, count, rev, dir_id):
        """Check for hash revision computation divergence.

           The Rationale behind this is that svn can trigger unknown
           edge cases (mixed CRLF, svn properties, etc...).  Those are
           not always easy to spot. Adding a check will help in
           spotting missing edge cases.

        Args:
            count (int): The number of revisions done so far
            rev (dict): The actual revision we are computing from
            dir_id (bytes): The actual directory for the given revision

        Returns:
            False if no hash divergence detected

        Raises
            ValueError if a hash divergence is detected

        """
        if (count % self.check_revision) == 0:  # hash computation check
            self.log.debug("Checking hash computations on revision %s..." % rev)
            checked_dir_id = self.swh_revision_hash_tree_at_svn_revision(rev)
            if checked_dir_id != dir_id:
                err = (
                    "Hash tree computation divergence detected "
                    "(%s != %s), stopping!"
                    % (
                        hashutil.hash_to_hex(dir_id),
                        hashutil.hash_to_hex(checked_dir_id),
                    )
                )
                raise ValueError(err)

    def process_svn_revisions(
        self, svnrepo, revision_start, revision_end, revision_parents
    ) -> Iterator[
        Tuple[List[Content], List[SkippedContent], List[Directory], Revision]
    ]:
        """Process svn revisions from revision_start to revision_end.

        At each svn revision, apply new diffs and simultaneously
        compute swh hashes.  This yields those computed swh hashes as
        a tuple (contents, directories, revision).

        Note that at every `self.check_revision`, a supplementary
        check takes place to check for hash-tree divergence (related
        T570).

        Yields:
            tuple (contents, directories, revision) of dict as a
            dictionary with keys, sha1_git, sha1, etc...

        Raises:
            ValueError in case of a hash divergence detection

        """
        gen_revs = svnrepo.swh_hash_data_per_revision(revision_start, revision_end)
        swh_revision = None
        count = 0
        for rev, nextrev, commit, new_objects, root_directory in gen_revs:
            count += 1
            # Send the associated contents/directories
            _contents, _skipped_contents, _directories = new_objects

            # compute the fs tree's checksums
            dir_id = root_directory.hash
            swh_revision = self.build_swh_revision(
                rev, commit, dir_id, revision_parents[rev]
            )

            self.log.debug(
                "rev: %s, swhrev: %s, dir: %s"
                % (
                    rev,
                    hashutil.hash_to_hex(swh_revision.id),
                    hashutil.hash_to_hex(dir_id),
                )
            )

            if self.check_revision:
                self._check_revision_divergence(count, rev, dir_id)

            if nextrev:
                revision_parents[nextrev] = [swh_revision.id]

            yield _contents, _skipped_contents, _directories, swh_revision

    def prepare_origin_visit(self, *args, **kwargs):
        self.origin = Origin(url=self.origin_url if self.origin_url else self.svn_url)

    def prepare(self, *args, **kwargs):
        latest_snapshot_revision = self._latest_snapshot_revision(self.origin_url)
        if latest_snapshot_revision:
            self.latest_snapshot, self.latest_revision = latest_snapshot_revision

        if self.destination_path:
            local_dirname = self.destination_path
        else:
            local_dirname = tempfile.mkdtemp(
                suffix="-%s" % os.getpid(),
                prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                dir=self.temp_directory,
            )

        self.svnrepo = svn.SvnRepo(
            self.svn_url, self.origin_url, local_dirname, self.max_content_length
        )

        try:
            revision_start, revision_end, revision_parents = self.start_from(
                self.start_from_scratch
            )
            self.swh_revision_gen = self.process_svn_revisions(
                self.svnrepo, revision_start, revision_end, revision_parents
            )
        except SvnLoaderUneventful as e:
            self.log.warning(e)
            if self.latest_snapshot:
                self._snapshot = self.latest_snapshot
            self.done = True
            self._load_status = "uneventful"
        except SvnLoaderHistoryAltered as e:
            self.log.error(e)
            self.done = True
            self._visit_status = "partial"

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
            self._load_status = "eventful"
        except StopIteration:
            self.done = True
            self._visit_status = "full"
            return False  # Stopping iteration
        except Exception as e:  # svn:external, hash divergence, i/o error...
            self.log.exception(e)
            self.done = True
            self._visit_status = "partial"
            return False  # Stopping iteration
        self._contents, self._skipped_contents, self._directories, rev = data
        if rev:
            self._last_revision = rev
            self._revisions.append(rev)
        return True  # next svn revision

    def store_data(self):
        """We store the data accumulated in internal instance variable.  If
           the iteration over the svn revisions is done, we create the
           snapshot and flush to storage the data.

           This also resets the internal instance variable state.

        """
        self.storage.skipped_content_add(self._skipped_contents)
        self.storage.content_add(self._contents)
        self.storage.directory_add(self._directories)
        self.storage.revision_add(self._revisions)

        if self.done:  # finish line, snapshot!
            self.snapshot = self.generate_and_load_snapshot(
                revision=self._last_revision, snapshot=self._snapshot
            )
            self.flush()
            self.loaded_snapshot_id = self.snapshot.id

        # reset internal state for next iteration
        self._revisions = []

    def generate_and_load_snapshot(
        self, revision: Optional[Revision] = None, snapshot: Optional[Snapshot] = None
    ) -> Snapshot:
        """Create the snapshot either from existing revision or snapshot.

        Revision (supposedly new) has priority over the snapshot
        (supposedly existing one).

        Args:
            revision (dict): Last revision seen if any (None by default)
            snapshot (dict): Snapshot to use if any (None by default)

        Returns:
            Optional[Snapshot] The newly created snapshot

        """
        if revision:  # Priority to the revision
            snap = Snapshot(
                branches={
                    DEFAULT_BRANCH: SnapshotBranch(
                        target=revision.id, target_type=TargetType.REVISION
                    )
                }
            )
        elif snapshot:  # Fallback to prior snapshot
            snap = snapshot
        else:
            raise ValueError(
                "generate_and_load_snapshot called with null revision and snapshot!"
            )
        self.log.debug("snapshot: %s" % snap)
        self.storage.snapshot_add([snap])
        return snap

    def load_status(self):
        return {
            "status": self._load_status,
        }

    def visit_status(self):
        return self._visit_status


class SvnLoaderFromDumpArchive(SvnLoader):
    """Uncompress an archive containing an svn dump, mount the svn dump as
       an svn repository and load said repository.

    """

    def __init__(
        self,
        url,
        archive_path,
        origin_url=None,
        destination_path=None,
        swh_revision=None,
        start_from_scratch=None,
        visit_date=None,
    ):
        super().__init__(
            url,
            origin_url=origin_url,
            destination_path=destination_path,
            swh_revision=swh_revision,
            start_from_scratch=start_from_scratch,
            visit_date=visit_date,
        )
        self.archive_path = archive_path
        self.temp_dir = None
        self.repo_path = None

    def prepare(self, *args, **kwargs):
        self.log.info("Archive to mount and load %s" % self.archive_path)
        self.temp_dir, self.repo_path = init_svn_repo_from_archive_dump(
            self.archive_path,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix="-%s" % os.getpid(),
            root_dir=self.temp_directory,
        )
        super().prepare(*args, **kwargs)

    def cleanup(self):
        super().cleanup()

        if self.temp_dir and os.path.exists(self.temp_dir):
            msg = "Clean up temporary directory dump %s for project %s" % (
                self.temp_dir,
                os.path.basename(self.repo_path),
            )
            self.log.debug(msg)
            shutil.rmtree(self.temp_dir)


class SvnLoaderFromRemoteDump(SvnLoader):
    """
    Create a subversion repository dump using the svnrdump utility,
    mount it locally and load the repository from it.
    """

    def __init__(
        self,
        url,
        origin_url=None,
        destination_path=None,
        swh_revision=None,
        start_from_scratch=False,
        visit_date=None,
    ):
        super().__init__(
            url,
            origin_url=origin_url,
            destination_path=destination_path,
            swh_revision=swh_revision,
            start_from_scratch=start_from_scratch,
            visit_date=visit_date,
        )
        self.temp_dir = tempfile.mkdtemp(dir=self.temp_directory)
        self.repo_path = None
        self.truncated_dump = False

    def get_last_loaded_svn_rev(self, svn_url: str) -> int:
        """Check if the svn repository has already been visited and return the last
        loaded svn revision number or -1 otherwise.

        """
        origin = list(self.storage.origin_get([svn_url]))[0]
        if not origin:
            return -1
        svn_revision = -1
        try:
            latest_snapshot_revision = self._latest_snapshot_revision(origin.url)
            if latest_snapshot_revision:
                _, latest_revision = latest_snapshot_revision
            latest_revision_headers = dict(latest_revision.extra_headers)
            svn_revision = int(latest_revision_headers[b"svn_revision"])
        except Exception:
            pass
        return svn_revision

    def dump_svn_revisions(self, svn_url, last_loaded_svn_rev=-1):
        """
        Generate a subversion dump file using the svnrdump tool.
        If the svnrdump command failed somehow,
        the produced dump file is analyzed to determine if a partial
        loading is still feasible.
        """
        # Build the svnrdump command line
        svnrdump_cmd = ["svnrdump", "dump", svn_url]

        # Launch the svnrdump command while capturing stderr as
        # successfully dumped revision numbers are printed to it
        dump_temp_dir = tempfile.mkdtemp(dir=self.temp_dir)
        dump_name = "".join(c for c in svn_url if c.isalnum())
        dump_path = "%s/%s.svndump" % (dump_temp_dir, dump_name)
        stderr_lines = []
        self.log.debug("Executing %s" % " ".join(svnrdump_cmd))
        with open(dump_path, "wb") as dump_file:
            stderr_r, stderr_w = pty.openpty()
            svnrdump = Popen(svnrdump_cmd, stdout=dump_file, stderr=stderr_w)
            os.close(stderr_w)
            stderr_stream = OutputStream(stderr_r)
            readable = True
            while readable:
                lines, readable = stderr_stream.read_lines()
                stderr_lines += lines
                for line in lines:
                    self.log.debug(line)
            svnrdump.wait()
            os.close(stderr_r)

        if svnrdump.returncode == 0:
            return dump_path

        # There was an error but it does not mean that no revisions
        # can be loaded.

        # Get the stderr line with latest dumped revision
        last_dumped_rev = None
        if len(stderr_lines) > 1:
            last_dumped_rev = stderr_lines[-2]

        if last_dumped_rev:
            # Get the latest dumped revision number
            matched_rev = re.search(".*revision ([0-9]+)", last_dumped_rev)
            last_dumped_rev = int(matched_rev.group(1)) if matched_rev else -1
            # Check if revisions inside the dump file can be loaded anyway
            if last_dumped_rev > last_loaded_svn_rev:
                self.log.debug(
                    (
                        "svnrdump did not dump all expected revisions "
                        "but revisions range %s:%s are available in "
                        "the generated dump file and will be loaded "
                        "into the archive."
                    )
                    % (last_loaded_svn_rev + 1, last_dumped_rev)
                )
                # Truncate the dump file after the last successfully dumped
                # revision to avoid the loading of corrupted data
                self.log.debug(
                    (
                        "Truncating dump file after the last "
                        "successfully dumped revision (%s) to avoid "
                        "the loading of corrupted data"
                    )
                    % last_dumped_rev
                )

                with open(dump_path, "r+b") as f:
                    with mmap(f.fileno(), 0, access=ACCESS_WRITE) as s:
                        pattern = (
                            "Revision-number: %s" % (last_dumped_rev + 1)
                        ).encode()
                        n = s.rfind(pattern)
                        if n != -1:
                            s.resize(n)
                self.truncated_dump = True
                return dump_path
            elif last_dumped_rev != -1:
                raise Exception(
                    (
                        "Last dumped subversion revision (%s) is "
                        "lesser than the last one loaded into the "
                        "archive (%s)."
                    )
                    % (last_dumped_rev, last_loaded_svn_rev)
                )

        raise Exception(
            "An error occurred when running svnrdump and "
            "no exploitable dump file has been generated."
        )

    def prepare(self, *args, **kwargs):
        # First, check if previous revisions have been loaded for the
        # subversion origin and get the number of the last one
        last_loaded_svn_rev = self.get_last_loaded_svn_rev(self.svn_url)

        # Then try to generate a dump file containing relevant svn revisions
        # to load, an exception will be thrown if something wrong happened
        dump_path = self.dump_svn_revisions(self.svn_url, last_loaded_svn_rev)

        # Finally, mount the dump and load the repository
        self.log.debug('Mounting dump file with "svnadmin load".')
        _, self.repo_path = init_svn_repo_from_dump(
            dump_path,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix="-%s" % os.getpid(),
            root_dir=self.temp_dir,
        )
        self.svn_url = "file://%s" % self.repo_path
        super().prepare(*args, **kwargs)

    def cleanup(self):
        super().cleanup()
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def visit_status(self):
        if self.truncated_dump:
            return "partial"
        else:
            return super().visit_status()
