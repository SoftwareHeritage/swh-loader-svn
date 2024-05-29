# Copyright (C) 2015-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Loader in charge of injecting either new or existing svn mirrors to
swh-storage.

"""
import atexit
from datetime import datetime
import difflib
import os
import pty
import re
import shutil
import signal
from subprocess import PIPE, Popen
import tempfile
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse

import requests
from subvertpy import SubversionException

from swh.loader.core.loader import BaseLoader
from swh.loader.core.utils import clean_dangling_folders
from swh.loader.exception import NotFound
from swh.loader.svn.svn_repo import SvnRepo, get_svn_repo
from swh.model import from_disk, hashutil
from swh.model.model import (
    Content,
    Directory,
    Revision,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

from . import converters
from .exception import SvnLoaderHistoryAltered, SvnLoaderUneventful
from .utils import OutputStream, init_svn_repo_from_dump, svn_urljoin

DEFAULT_BRANCH = b"HEAD"
TEMPORARY_DIR_PREFIX_PATTERN = "swh.loader.svn."
SUBVERSION_ERROR = re.compile(r".*(E[0-9]{6}):.*")
SUBVERSION_NOT_FOUND = "E170013"


class SvnLoader(BaseLoader):
    """SVN loader. The repository is either remote or local. The loader deals with
    update on an already previously loaded repository.

    """

    visit_type = "svn"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        origin_url: Optional[str] = None,
        visit_date: Optional[datetime] = None,
        incremental: bool = True,
        temp_directory: str = "/tmp",
        debug: bool = False,
        check_revision: int = 0,
        check_revision_from: int = 0,
        **kwargs: Any,
    ):
        """Load a svn repository (either remote or local).

        Args:
            url: The default origin url
            origin_url: Optional original url override to use as origin reference in the
                archive. If not provided, "url" is used as origin.
            visit_date: Optional date to override the visit date
            incremental: If True, the default, starts from the last snapshot (if any).
                Otherwise, starts from the initial commit of the repository.
            temp_directory: The temporary directory to use as root directory for working
                directory computations
            debug: If true, run the loader in debug mode. At the end of the loading, the
                temporary working directory is not cleaned up to ease inspection.
                Defaults to false.
            check_revision: The number of svn commits between checks for hash divergence

        """
        # technical svn uri to act on svn repository
        self.svn_url = url
        # origin url as unique identifier for origin in swh archive
        origin_url = origin_url or self.svn_url
        super().__init__(storage=storage, origin_url=origin_url, **kwargs)
        self.debug = debug
        self.temp_directory = temp_directory
        self.done = False
        self.svnrepo = None
        self.skip_post_load = False
        # Revision check is configurable
        self.check_revision = check_revision
        self.check_revision_from = check_revision_from
        # internal state used to store swh objects
        self._contents: List[Content] = []
        self._skipped_contents: List[SkippedContent] = []
        self._directories: List[Directory] = []
        self._revisions: List[Revision] = []
        self._snapshot: Optional[Snapshot] = None
        # internal state, current visit
        self._last_revision = None
        self._visit_status = "full"
        self._load_status = "uneventful"
        self.visit_date = visit_date or self.visit_date
        self.incremental = incremental
        self.snapshot: Optional[Snapshot] = None
        # state from previous visit
        self.latest_snapshot = None
        self.latest_revision: Optional[Revision] = None

        def kill_child_processes():
            try:
                os.killpg(os.getpid(), signal.SIGTERM)
            except (SystemExit, ProcessLookupError):
                # ignore SystemExit raised by swh.core.cli.clean_exit_on_signal
                # when loader is executed through swh CLI
                pass

        # ensure to terminate svn* child processes that might have been
        # spawned by the loader if its process gets killed
        atexit.register(kill_child_processes)

    def svn_repo(
        self, remote_url: str, origin_url: Optional[str], temp_dir: str
    ) -> Optional[SvnRepo]:
        parsed_remote_url = urlparse(remote_url)
        if (
            parsed_remote_url.netloc == "svn.code.sf.net"
            and parsed_remote_url.scheme.startswith("http")
        ):
            # if remote svn URL hosted on sourceforge has http(s) scheme, we want
            # to check if the communication with the repository can be done with
            # the svn protocol as it is much faster
            svn_urls = [
                urlunparse(parsed_remote_url._replace(scheme="svn")),
                remote_url,
            ]
        else:
            svn_urls = [remote_url]

        last_exc: Optional[Exception] = None
        svnrepo = None
        for svn_url in svn_urls:
            try:
                svnrepo = get_svn_repo(
                    svn_url,
                    origin_url,
                    temp_dir,
                    self.max_content_size,
                    debug=self.debug,
                )
                break
            except (NotFound, SubversionException) as exc:
                # Keep trying until the last URL in the list
                last_exc = exc
        else:
            if last_exc:
                raise last_exc
        return svnrepo

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
        """Clean up the svn repository's working representation on disk."""
        if not self.svnrepo:  # could happen if `prepare` fails
            return
        if self.debug:
            self.log.error(
                """NOT FOR PRODUCTION - debug flag activated
Local repository not cleaned up for investigation: %s""",
                self.svnrepo.local_url.decode("utf-8"),
            )
            return
        self.svnrepo.clean_fs()

    def swh_revision_hash_tree_at_svn_revision(
        self, revision: int
    ) -> from_disk.Directory:
        """Compute and return the hash tree at a given svn revision.

        Args:
            rev: the svn revision we want to check

        Returns:
            The hash tree directory as bytes.

        """
        assert self.svnrepo is not None
        local_dirname, local_url = self.svnrepo.export_temporary(revision)
        root_dir = from_disk.Directory.from_disk(
            path=local_url, max_content_length=self.max_content_size
        )
        self.svnrepo.clean_fs(local_dirname)
        return root_dir

    def _latest_snapshot_revision(
        self,
        origin_url: str,
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
        latest_snapshot = snapshot_get_latest(
            storage, origin_url, visit_type=self.visit_type
        )
        if not latest_snapshot:
            return None
        branches = latest_snapshot.branches
        if not branches:
            return None
        branch = branches.get(DEFAULT_BRANCH)
        if not branch:
            return None
        if branch.target_type != SnapshotTargetType.REVISION:
            return None
        swh_id = branch.target

        revision = storage.revision_get([swh_id])[0]
        if not revision:
            return None
        return latest_snapshot, revision

    def build_swh_revision(
        self, rev: int, commit: Dict, dir_id: bytes, parents: Sequence[bytes]
    ) -> Revision:
        """Build the swh revision dictionary.

        This adds:

        - the `'synthetic`' flag to true
        - the '`extra_headers`' containing the repository's uuid and the
          svn revision number.

        Args:
            rev: the svn revision number
            commit: the commit data: revision id, date, author, and message
            dir_id: the upper tree's hash identifier
            parents: the parents' identifiers

        Returns:
            The swh revision corresponding to the svn revision.

        """
        assert self.svnrepo is not None
        return converters.build_swh_revision(
            rev, commit, self.svnrepo.uuid, dir_id, parents
        )

    def check_history_not_altered(self, revision_start: int, swh_rev: Revision) -> bool:
        """Given a svn repository, check if the history was modified in between visits."""

        self.log.debug("Checking if history of repository got altered since last visit")

        revision_id = swh_rev.id
        parents = swh_rev.parents

        assert self.svnrepo is not None
        commit, root_dir = self.svnrepo.swh_hash_data_at_revision(revision_start)

        dir_id = root_dir.hash
        swh_revision = self.build_swh_revision(revision_start, commit, dir_id, parents)
        swh_revision_id = swh_revision.id
        return swh_revision_id == revision_id

    def start_from(self) -> Tuple[int, int]:
        """Determine from where to start the loading.

        Returns:
            tuple (revision_start, revision_end)

        Raises:

            SvnLoaderHistoryAltered: When a hash divergence has been
                                     detected (should not happen)
            SvnLoaderUneventful: Nothing changed since last visit

        """
        assert self.svnrepo is not None, "svnrepo initialized in the `prepare` method"
        revision_head = self.svnrepo.head_revision()
        if revision_head == 0:  # empty repository case
            revision_start = 0
            revision_end = 0
        else:  # default configuration
            revision_start = self.svnrepo.initial_revision()
            revision_end = revision_head

        # start from a previous revision if any
        if self.incremental and self.latest_revision is not None:
            extra_headers = dict(self.latest_revision.extra_headers)
            revision_start = int(extra_headers[b"svn_revision"])

            if not self.check_history_not_altered(revision_start, self.latest_revision):
                self.log.debug(
                    (
                        "History of svn %s@%s altered. "
                        "A complete reloading of the repository will be performed."
                    ),
                    self.svnrepo.remote_url,
                    revision_start,
                )
                revision_start = 0

            # now we know history is ok, we start at next revision
            revision_start = revision_start + 1

        if revision_start > revision_end:
            msg = "%s@%s already injected." % (self.svnrepo.remote_url, revision_end)
            raise SvnLoaderUneventful(msg)

        self.log.info(
            "Processing revisions [%s-%s] for %s",
            revision_start,
            revision_end,
            self.svnrepo,
        )

        return revision_start, revision_end

    def _check_revision_divergence(
        self, rev: int, dir_id: bytes, dir: from_disk.Directory
    ) -> None:
        """Check for hash revision computation divergence.

           The Rationale behind this is that svn can trigger unknown edge cases (mixed
           CRLF, svn properties, etc...). Those are not always easy to spot. Adding a
           regular check will help spotting potential missing edge cases.

        Args:
            rev: The actual revision we are computing from
            dir_id: The actual directory for the given revision

        Raises
            ValueError if a hash divergence is detected

        """

        self.log.debug("Checking hash computations on revision %s...", rev)
        checked_dir = self.swh_revision_hash_tree_at_svn_revision(rev)
        checked_dir_id = checked_dir.hash

        if checked_dir_id != dir_id:
            # do not bother checking tree differences if root directory id of reconstructed
            # repository filesystem does not match the id of the one from the last loaded
            # revision (can happen when called from post_load and tree differences were checked
            # before the last revision to load)
            if self.debug and dir_id == dir.hash:
                for obj in checked_dir.iter_tree():
                    path = obj.data["path"].replace(
                        checked_dir.data["path"] + b"/", b""
                    )
                    if not path:
                        # ignore root directory
                        continue
                    if path not in dir:
                        self.log.debug(
                            "%s with path %s is missing in reconstructed repository filesystem",
                            obj.object_type,
                            path,
                        )
                    elif dir[path].hash != checked_dir[path].hash:
                        self.log.debug(
                            "%s with path %s has different hash in reconstructed repository filesystem",  # noqa
                            obj.object_type,
                            path,
                        )
                        if obj.object_type == "content":
                            self.log.debug(
                                "expected sha1: %s, actual sha1: %s",
                                hashutil.hash_to_hex(checked_dir[path].data["sha1"]),
                                hashutil.hash_to_hex(dir[path].data["sha1"]),
                            )
                            # compute and display diff between contents
                            file_path = (
                                checked_dir[path]
                                .data["path"]
                                .replace(checked_dir.data["path"], b"")
                            ).decode()
                            with tempfile.TemporaryDirectory() as tmpdir:
                                export_path = os.path.join(
                                    tmpdir, os.path.basename(file_path)
                                )
                                assert self.svnrepo is not None
                                self.svnrepo.export(
                                    url=svn_urljoin(self.svnrepo.remote_url, file_path),
                                    to=export_path,
                                    rev=rev,
                                    peg_rev=rev,
                                    ignore_keywords=True,
                                    overwrite=True,
                                )
                                with (
                                    open(export_path, "rb") as exported_file,
                                    open(dir[path].data["path"], "rb") as checkout_file,
                                ):
                                    diff_lines = difflib.diff_bytes(
                                        difflib.unified_diff,
                                        exported_file.read().split(b"\n"),
                                        checkout_file.read().split(b"\n"),
                                    )
                                    self.log.debug(
                                        "below is diff between files:\n"
                                        + os.fsdecode(b"\n".join(list(diff_lines)[2:]))
                                    )

            err = (
                "Hash tree computation divergence detected at revision %s "
                "(%s != %s), stopping!"
                % (
                    rev,
                    hashutil.hash_to_hex(dir_id),
                    hashutil.hash_to_hex(checked_dir_id),
                )
            )
            raise ValueError(err)

    def process_svn_revisions(
        self, svnrepo, revision_start, revision_end
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
        parents = (self.latest_revision.id,) if self.latest_revision is not None else ()
        count = 0
        for rev, commit, new_objects, root_directory in gen_revs:
            count += 1
            # Send the associated contents/directories
            _contents, _skipped_contents, _directories = new_objects

            # compute the fs tree's checksums
            dir_id = root_directory.hash
            swh_revision = self.build_swh_revision(rev, commit, dir_id, parents)

            self.log.debug(
                "rev: %s, swhrev: %s, dir: %s",
                rev,
                hashutil.hash_to_hex(swh_revision.id),
                hashutil.hash_to_hex(dir_id),
            )

            if (
                self.check_revision
                and rev >= self.check_revision_from
                and count % self.check_revision == 0
            ):
                self._check_revision_divergence(rev, dir_id, root_directory)

            parents = (swh_revision.id,)

            yield _contents, _skipped_contents, _directories, swh_revision

        if not self.debug and self.svnrepo:
            # clean directory where revisions were replayed to gain some disk space
            # before the post_load operation
            self.svnrepo.clean_fs(self.svnrepo.local_url)

    def prepare(self):
        latest_snapshot_revision = self._latest_snapshot_revision(self.origin.url)
        if latest_snapshot_revision:
            self.latest_snapshot, self.latest_revision = latest_snapshot_revision
            self._snapshot = self.latest_snapshot
            if self.incremental:
                self._last_revision = self.latest_revision
            else:
                self.latest_revision = None

        local_dirname = self._create_tmp_dir(self.temp_directory)

        self.svnrepo = self.svn_repo(
            self.svn_url,
            self.origin.url,
            local_dirname,
        )

        try:
            revision_start, revision_end = self.start_from()
            self.swh_revision_gen = self.process_svn_revisions(
                self.svnrepo, revision_start, revision_end
            )
        except SvnLoaderUneventful as e:
            self.log.warning(e)
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

        if self.done:
            return False

        try:
            data = next(self.swh_revision_gen)
            self._load_status = "eventful"
        except StopIteration:
            self.done = True  # Stopping iteration
            self._visit_status = "full"
        except Exception as e:  # svn:external, hash divergence, i/o error...
            self.log.exception(e)
            self.done = True  # Stopping iteration
            self._visit_status = "partial"
        else:
            self._contents, self._skipped_contents, self._directories, rev = data
            if rev:
                self._last_revision = rev
                self._revisions.append(rev)
        return not self.done

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
            if (
                self.latest_snapshot
                and self.latest_snapshot.id == self.loaded_snapshot_id
            ):
                # no new objects to archive found during the visit
                self._load_status = "uneventful"

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
                        target=revision.id, target_type=SnapshotTargetType.REVISION
                    )
                }
            )
        elif snapshot:  # Fallback to prior snapshot
            snap = snapshot
        else:
            raise ValueError(
                "generate_and_load_snapshot called with null revision and snapshot!"
            )
        self.log.debug("snapshot: %s", snap)
        self.storage.snapshot_add([snap])
        return snap

    def load_status(self):
        return {
            "status": self._load_status,
        }

    def visit_status(self):
        return self._visit_status

    def post_load(self, success: bool = True) -> None:
        if self.skip_post_load:
            return
        if success and self._last_revision is not None:
            # check if the reconstructed filesystem for the last loaded revision is
            # consistent with the one obtained with a svn export operation. If it is not
            # the case, an exception will be raised to report the issue and mark the
            # visit as partial
            self._check_revision_divergence(
                int(dict(self._last_revision.extra_headers)[b"svn_revision"]),
                self._last_revision.directory,
                self.svnrepo.swhreplay.directory,
            )

    def _create_tmp_dir(self, root_tmp_dir: str) -> str:
        return tempfile.mkdtemp(
            dir=root_tmp_dir,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix="-%s" % os.getpid(),
        )


class SvnLoaderFromDump(SvnLoader):
    """Mount a (possibly gzip compressed) svn dump as a local svn repository
    and load that repository.
    """

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        dump_path: str,
        gzip_dump: bool = True,
        origin_url: Optional[str] = None,
        incremental: bool = False,
        visit_date: Optional[datetime] = None,
        temp_directory: str = "/tmp",
        debug: bool = False,
        check_revision: int = 0,
        **kwargs: Any,
    ):
        super().__init__(
            storage=storage,
            url=url,
            origin_url=origin_url,
            incremental=incremental,
            visit_date=visit_date,
            temp_directory=temp_directory,
            debug=debug,
            check_revision=check_revision,
            **kwargs,
        )
        self.dump_path = dump_path
        self.gzip_dump = gzip_dump
        self.temp_dir = None
        self.repo_path = None

    def prepare(self):
        cleanup_dump = False
        if self.dump_path.startswith("http"):
            self.log.debug("Downloading dump file from URL %s", self.dump_path)
            dump_path = os.path.join(tempfile.mkdtemp(), "repo_dump")
            response = requests.get(self.dump_path, stream=True)
            with open(dump_path, "wb") as dump:
                for chunk in response.iter_content(chunk_size=hashutil.HASH_BLOCK_SIZE):
                    dump.write(chunk)
            self.dump_path = dump_path
            cleanup_dump = True
        self.log.info("Archive to mount and load %s", self.dump_path)
        self.temp_dir, self.repo_path = init_svn_repo_from_dump(
            self.dump_path,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix="-%s" % os.getpid(),
            root_dir=self.temp_directory,
            cleanup_dump=cleanup_dump,
            gzip=self.gzip_dump,
        )
        self.svn_url = f"file://{self.repo_path}"
        super().prepare()

    def cleanup(self):
        super().cleanup()

        if self.temp_dir and os.path.exists(self.temp_dir):
            self.log.debug(
                "Clean up temporary directory dump %s for project %s",
                self.temp_dir,
                os.path.basename(self.repo_path),
            )
            shutil.rmtree(self.temp_dir)

    def svn_repo(
        self, remote_url: str, origin_url: Optional[str], temp_dir: str
    ) -> Optional[SvnRepo]:
        return super().svn_repo(
            remote_url,
            # consider remote origin URL as dead when using SvnLoaderFromDump
            # to avoid slow connection timeouts with retries in SvnRepo constructor
            (
                origin_url
                if origin_url and not origin_url.startswith(("http", "svn"))
                else None
            ),
            temp_dir,
        )


class SvnLoaderFromRemoteDump(SvnLoader):
    """Create a subversion repository dump out of a remote svn repository (using the
    svnrdump utility). Then, mount the repository locally and load that repository.

    """

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        origin_url: Optional[str] = None,
        incremental: bool = True,
        visit_date: Optional[datetime] = None,
        temp_directory: str = "/tmp",
        debug: bool = False,
        check_revision: int = 0,
        **kwargs: Any,
    ):
        super().__init__(
            storage=storage,
            url=url,
            origin_url=origin_url,
            incremental=incremental,
            visit_date=visit_date,
            temp_directory=temp_directory,
            debug=debug,
            check_revision=check_revision,
            **kwargs,
        )
        self.temp_dir = self._create_tmp_dir(self.temp_directory)
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

    def dump_svn_revisions(
        self, svn_url: str, max_rev: int = -1, last_loaded_svn_rev: int = -1
    ) -> Tuple[str, int]:
        """Generate a compressed subversion dump file using the svnrdump tool and gzip.
        If the svnrdump command failed somehow, the produced dump file is analyzed to
        determine if a partial loading is still feasible.

        Raises:
            NotFound when the repository is no longer found at url

        Returns:
            The dump_path of the repository mounted and the max dumped revision number
            (-1 if all revisions were dumped)
        """
        # Build the svnrdump command line
        svnrdump_cmd = ["svnrdump", "dump", svn_url]
        assert self.svnrepo is not None
        if self.svnrepo.username:
            svnrdump_cmd += [
                "--username",
                self.svnrepo.username,
                "--password",
                self.svnrepo.password,
            ]
        if max_rev > 0:
            svnrdump_cmd.append(f"-r0:{max_rev}")

        # Launch the svnrdump command while capturing stderr as
        # successfully dumped revision numbers are printed to it
        dump_temp_dir = tempfile.mkdtemp(dir=self.temp_dir)
        dump_name = "".join(c for c in svn_url if c.isalnum())
        dump_path = "%s/%s.svndump.gz" % (dump_temp_dir, dump_name)
        stderr_lines = []
        self.log.debug("Executing %s", " ".join(svnrdump_cmd))
        with open(dump_path, "wb") as dump_file:
            gzip = Popen(["gzip"], stdin=PIPE, stdout=dump_file)
            stderr_r, stderr_w = pty.openpty()
            svnrdump = Popen(svnrdump_cmd, stdout=gzip.stdin, stderr=stderr_w)
            os.close(stderr_w)
            stderr_stream = OutputStream(stderr_r)
            readable = True
            error_codes: List[str] = []
            error_messages: List[str] = []
            while readable:
                lines, readable = stderr_stream.read_lines()
                stderr_lines += lines
                for line in lines:
                    self.log.debug(line)
                    match = SUBVERSION_ERROR.search(line)
                    if match:
                        error_codes.append(match.group(1))
                        error_messages.append(line)
            svnrdump.wait()
            os.close(stderr_r)
            # denote end of read file
            gzip.stdin.close()
            gzip.wait()

        if svnrdump.returncode == 0:
            return dump_path, -1

        # There was an error but it does not mean that no revisions
        # can be loaded.

        # Get the stderr line with latest dumped revision
        last_dumped_rev = None
        for stderr_line in reversed(stderr_lines):
            if stderr_line.startswith("* Dumped revision"):
                last_dumped_rev = stderr_line
                break

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
                    ),
                    last_loaded_svn_rev + 1,
                    last_dumped_rev,
                )
                self.truncated_dump = True
                return dump_path, last_dumped_rev
            elif last_dumped_rev != -1 and last_dumped_rev < last_loaded_svn_rev:
                raise Exception(
                    (
                        "Last dumped subversion revision (%s) is "
                        "lesser than the last one loaded into the "
                        "archive (%s)."
                    )
                    % (last_dumped_rev, last_loaded_svn_rev)
                )

        if SUBVERSION_NOT_FOUND in error_codes:
            raise NotFound(
                f"{SUBVERSION_NOT_FOUND}: Repository never existed or disappeared"
            )

        raise Exception(
            "An error occurred when running svnrdump and "
            "no exploitable dump file has been generated.\n" + "\n".join(error_messages)
        )

    def prepare(self):
        # First, check if previous revisions have been loaded for the
        # subversion origin and get the number of the last one
        last_loaded_svn_rev = self.get_last_loaded_svn_rev(self.origin.url)

        self.svnrepo = self.svn_repo(self.origin.url, self.origin.url, self.temp_dir)

        # Ensure to use remote URL retrieved by SvnRepo as origin URL might redirect
        # and svnrdump does not handle URL redirection
        self.svn_url = self.svnrepo.remote_url

        max_rev = -1
        if self.svnrepo.root_directory:
            # When loading a sub-path of a repository from a dump file it has been
            # observed it is less error-prone to dump the whole repository as some
            # partial dumps fail to be loaded by svnadmin or svnrdump can end up
            # with error
            try:
                self.svnrepo.info(self.svnrepo.repos_root_url)
            except SubversionException:
                # Repository root URL cannot be accessed by a svn client, try to
                # load from a partial dump then
                pass
            else:
                # A dump file for the whole repository can be produced, in that case
                # we stop to dump revisions once the last one modifying the repository
                # sub-path was dumped (revisions not modifying sub-path are then filtered
                # out during the loading process).

                path = self.svnrepo.remote_url.replace(
                    self.svnrepo.repos_root_url + "/", ""
                )
                svn_repo = self.svn_repo(
                    self.svnrepo.repos_root_url,
                    self.svnrepo.repos_root_url,
                    self.temp_dir,
                )
                path_last_log = list(
                    svn_repo.logs(
                        svn_repo.head_revision(),
                        0,
                        paths=[path],
                        discover_changed_paths=False,
                        limit=1,
                    )
                )
                max_rev = path_last_log[0]["rev"]
                self.svn_url = self.svnrepo.repos_root_url

        # Then for stale repository, check if the last loaded revision in the archive
        # is different from the last revision on the remote subversion server.
        # Skip the dump of all revisions and the loading process if they are identical
        # to save some disk space and processing time.
        last_loaded_snp_and_rev = self._latest_snapshot_revision(self.origin.url)
        if last_loaded_snp_and_rev is not None:
            last_loaded_snp, last_loaded_rev = last_loaded_snp_and_rev
            stale_repository = self.svnrepo.head_revision() == last_loaded_svn_rev
            if stale_repository and self.check_history_not_altered(
                last_loaded_svn_rev, last_loaded_rev
            ):
                self._snapshot = last_loaded_snp
                self._last_revision = last_loaded_rev
                self.done = True
                self.skip_post_load = True
                return

        # Then try to generate a dump file containing relevant svn revisions
        # to load, an exception will be thrown if something wrong happened
        dump_path, max_rev = self.dump_svn_revisions(
            self.svn_url, max_rev, last_loaded_svn_rev
        )

        # Finally, mount the dump and load the repository
        self.log.debug('Mounting dump file with "svnadmin load".')
        _, self.repo_path = init_svn_repo_from_dump(
            dump_path,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix="-%s" % os.getpid(),
            root_dir=self.temp_dir,
            gzip=True,
            max_rev=max_rev,
        )
        self.svn_url = "file://%s" % self.repo_path
        super().prepare()

    def cleanup(self):
        super().cleanup()
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def visit_status(self):
        if self.truncated_dump:
            return "partial"
        else:
            return super().visit_status()
