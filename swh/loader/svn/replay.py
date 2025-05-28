# Copyright (C) 2016-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Remote Access client to svn server."""

from __future__ import annotations

import codecs
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain
import logging
import os
import shutil
import tempfile
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from subvertpy import SubversionException, properties
from subvertpy.ra import RemoteAccess

from swh.model import from_disk
from swh.model.model import Content, Directory, SkippedContent

if TYPE_CHECKING:
    from swh.loader.svn.svn_repo import SvnRepo

from swh.loader.svn.utils import (
    ExternalDefinition,
    is_recursive_external,
    parse_external_definition,
    quote_svn_url,
    svn_urljoin,
)

logger = logging.getLogger(__name__)


def _ra_codecs_error_handler(e: UnicodeError) -> Tuple[Union[str, bytes], int]:
    """Subvertpy may fail to decode to utf-8 the user svn properties.  As
       they are not used by the loader, return an empty string instead
       of the decoded content.

    Args:
        e: exception raised during the svn properties decoding.

    """
    return "", cast(UnicodeDecodeError, e).end


DEFAULT_FLAG = 0
EXEC_FLAG = 1
NOEXEC_FLAG = 2

SVN_PROPERTY_EOL = "svn:eol-style"


class FileEditor:
    """File Editor in charge of updating file on disk and memory objects."""

    __slots__ = [
        "directory",
        "path",
        "fullpath",
        "executable",
        "link",
        "state",
        "svnrepo",
        "editor",
        "max_content_size",
    ]

    def __init__(
        self,
        directory: from_disk.Directory,
        rootpath: bytes,
        path: bytes,
        svnrepo: SvnRepo,
        max_content_size: Optional[int],
    ):
        self.directory = directory
        self.path = path
        self.fullpath = os.path.join(rootpath, path)
        self.svnrepo = svnrepo
        self.editor: Editor = svnrepo.swhreplay.editor
        self.max_content_size = max_content_size

    def change_prop(self, key: str, value: str) -> None:
        if self.editor.debug:
            logger.debug(
                "Setting property %s to value %s on path %s", key, value, self.path
            )

    def apply_textdelta(self, base_checksum) -> Callable[[Any, bytes, BinaryIO], None]:
        if self.editor.debug:
            logger.debug("Applying textdelta to file %s", self.path)
        # do not apply textdelta, file will be fully exported when closing the editor
        return lambda *args: None

    def close(self) -> None:
        """When done with a file added or modified in the current replayed revision,
        we export it to disk and update the from_disk model.

        """
        if self.editor.debug:
            logger.debug("Closing file %s", self.path)

        if self.path not in self.editor.external_paths:
            # export file to disk if its path does not match an external
            url = svn_urljoin(self.svnrepo.repos_root_url, os.fsdecode(self.path))
            self.svnrepo.export(
                url,
                to=self.fullpath,
                rev=self.editor.revnum,
                peg_rev=self.editor.revnum,
                ignore_keywords=True,
                overwrite=True,
            )

        # And now compute file's checksums
        self.directory[self.path] = from_disk.Content.from_file(
            path=self.fullpath, max_content_length=self.max_content_size
        )


@dataclass
class DirState:
    """Persists some directory states (eg. externals) across revisions while
    replaying them."""

    externals: Dict[str, List[ExternalDefinition]] = field(default_factory=dict)
    """Map a path in the directory to a list of (external_url, revision, relative_url)
    targeting it"""
    externals_paths: Set[bytes] = field(default_factory=set)
    """Keep track of all external paths reachable from the directory"""


class DirEditor:
    """Directory Editor in charge of updating directory hashes computation.

    This implementation includes empty folder in the hash computation.

    """

    __slots__ = [
        "directory",
        "rootpath",
        "path",
        "dir_states",
        "svnrepo",
        "editor",
        "externals",
        "max_content_size",
    ]

    def __init__(
        self,
        directory: from_disk.Directory,
        rootpath: bytes,
        path: bytes,
        dir_states: Dict[bytes, DirState],
        svnrepo: SvnRepo,
        max_content_size: Optional[int],
    ):
        self.directory = directory
        self.rootpath = rootpath
        self.path = path
        # build directory on init
        os.makedirs(rootpath, exist_ok=True)
        if path and path not in self.directory:
            self.directory[path] = from_disk.Directory()
        self.dir_states = dir_states
        self.svnrepo = svnrepo
        self.editor = svnrepo.swhreplay.editor
        self.externals: Dict[str, List[ExternalDefinition]] = {}
        self.max_content_size = max_content_size

    def remove_child(self, path: bytes) -> None:
        """Remove a path from the current objects.

        The path can be resolved as link, file or directory.

        This function takes also care of removing the link between the
        child and the parent.

        Args:
            path: to remove from the current objects.

        """
        if path in self.directory:
            if self.editor.debug:
                logger.debug("Removing path %s", path)
            entry_removed = self.directory[path]
            del self.directory[path]
            self.dir_states.pop(path, None)
            fpath = os.path.join(self.rootpath, path)
            if isinstance(entry_removed, from_disk.Directory):
                shutil.rmtree(fpath)
            else:
                os.remove(fpath)

    def open_directory(self, path: str, *args) -> DirEditor:
        """Updating existing directory."""
        if self.editor.debug:
            logger.debug("Opening directory %s", path)
        return DirEditor(
            self.directory,
            rootpath=self.rootpath,
            path=os.fsencode(path),
            dir_states=self.dir_states,
            svnrepo=self.svnrepo,
            max_content_size=self.max_content_size,
        )

    def add_directory(
        self, path: str, copyfrom_path: Optional[str] = None, copyfrom_rev: int = -1
    ) -> DirEditor:
        """Adding a new directory."""
        if self.editor.debug:
            logger.debug(
                "Adding directory %s, copyfrom_path = %s, copyfrom_rev = %s",
                path,
                copyfrom_path,
                copyfrom_rev,
            )

        path_bytes = os.fsencode(path)
        fullpath = os.path.join(self.rootpath, path_bytes)

        os.makedirs(fullpath, exist_ok=True)
        if copyfrom_rev == -1:
            if path_bytes and path_bytes not in self.directory:
                self.directory[path_bytes] = from_disk.Directory()
        else:
            url = svn_urljoin(self.svnrepo.repos_root_url, copyfrom_path)
            self.remove_child(path_bytes)
            self.svnrepo.export(
                url,
                to=fullpath,
                peg_rev=copyfrom_rev,
                ignore_keywords=True,
                overwrite=True,
                ignore_externals=True,
            )
            self.directory[path_bytes] = from_disk.Directory.from_disk(
                path=fullpath, max_content_length=self.max_content_size
            )

            # get externals for the copied paths possibly set in copyfrom_rev
            externals = self.svnrepo.propget(
                "svn:externals",
                url,
                peg_rev=copyfrom_rev,
                rev=copyfrom_rev,
                recurse=True,
            )

            assert copyfrom_path is not None
            copyfrom_path_bytes = os.fsencode(copyfrom_path).lstrip(b"/")
            copyfrom_fullpath = os.path.join(self.rootpath, copyfrom_path_bytes)

            def _set_dir_state(path: bytes, copied_path: bytes):
                url = svn_urljoin(self.svnrepo.repos_root_url, os.fsdecode(copied_path))
                if quote_svn_url(url) in externals:
                    # set externals state for copied directory
                    dir_editor = DirEditor(
                        self.directory,
                        self.rootpath,
                        path,
                        self.dir_states,
                        svnrepo=self.svnrepo,
                        max_content_size=self.max_content_size,
                    )
                    dir_editor.change_prop(
                        properties.PROP_EXTERNALS,
                        os.fsdecode(externals[quote_svn_url(url)]),
                    )
                    dir_editor.close()

            _set_dir_state(path_bytes, copyfrom_path_bytes)

            for root, dirs, _ in os.walk(fullpath):
                for dir in dirs:
                    dir_fullpath = os.path.join(root, dir)
                    copied_dir_fullpath = dir_fullpath.replace(
                        fullpath, copyfrom_fullpath
                    )
                    dir_path = dir_fullpath.replace(self.rootpath, b"").lstrip(b"/")
                    copied_dir_path = copied_dir_fullpath.replace(self.rootpath, b"")
                    _set_dir_state(dir_path, copied_dir_path.lstrip(b"/"))

        return DirEditor(
            self.directory,
            self.rootpath,
            path_bytes,
            self.dir_states,
            svnrepo=self.svnrepo,
            max_content_size=self.max_content_size,
        )

    def open_file(self, path: str, *args) -> FileEditor:
        """Updating existing file."""
        if self.editor.debug:
            logger.debug("Opening file %s", path)

        path_bytes = os.fsencode(path)
        self.directory[path_bytes] = from_disk.Content()
        return FileEditor(
            self.directory,
            rootpath=self.rootpath,
            path=path_bytes,
            svnrepo=self.svnrepo,
            max_content_size=self.max_content_size,
        )

    def add_file(
        self, path: str, copyfrom_path: Optional[str] = None, copyfrom_rev: int = -1
    ) -> FileEditor:
        """Creating a new file."""
        if self.editor.debug:
            logger.debug(
                "Adding file %s, copyfrom_path = %s, copyfrom_rev = %s",
                path,
                copyfrom_path,
                copyfrom_rev,
            )

        path_bytes = os.fsencode(path)
        fullpath = os.path.join(self.rootpath, path_bytes)

        if copyfrom_rev == -1:
            self.directory[path_bytes] = from_disk.Content()
        else:
            url = svn_urljoin(self.svnrepo.repos_root_url, copyfrom_path)
            self.remove_child(path_bytes)
            self.svnrepo.export(
                url,
                to=fullpath,
                peg_rev=copyfrom_rev,
                ignore_keywords=True,
                overwrite=True,
            )
            self.directory[path_bytes] = from_disk.Content.from_file(
                path=fullpath, max_content_length=self.max_content_size
            )

        return FileEditor(
            self.directory,
            self.rootpath,
            path_bytes,
            svnrepo=self.svnrepo,
            max_content_size=self.max_content_size,
        )

    def change_prop(self, key: str, value: str) -> None:
        """Change property callback on directory."""
        if key == properties.PROP_EXTERNALS:
            logger.debug(
                "Setting '%s' property with value '%s' on path %s",
                key,
                value,
                self.path,
            )
            self.externals = defaultdict(list)
            if value is not None:
                try:
                    # externals are set on that directory path, parse and store them
                    # for later processing in the close method
                    for external in value.split("\n"):
                        external = external.strip(" \t\r")
                        # skip empty line or comment
                        if not external or external.startswith("#"):
                            continue
                        external_def = parse_external_definition(
                            external, os.fsdecode(self.path), self.svnrepo.origin_url
                        )
                        self.externals[external_def.path].append(external_def)
                except ValueError:
                    logger.debug(
                        "Failed to parse external: %s\n"
                        "Externals defined on path %s will not be processed",
                        external,
                        self.path,
                    )
                    # as the official subversion client, do not process externals in case
                    # of parsing error
                    self.externals = {}

            if not self.externals:
                # externals might have been unset on that directory path,
                # remove associated paths from the reconstructed filesystem
                externals = self.dir_states[self.path].externals
                for path in externals.keys():
                    self.remove_external_path(os.fsencode(path))
                self.dir_states.pop(self.path)

    def delete_entry(self, path: str, revision: int) -> None:
        """Remove a path."""
        if self.editor.debug:
            logger.debug("Deleting directory entry %s", path)

        path_bytes = os.fsencode(path)
        fullpath = os.path.join(self.rootpath, path_bytes)

        if os.path.isdir(fullpath):
            # remove all external paths associated to the removed directory
            # (we cannot simply remove a root external directory as externals
            # paths associated to ancestor directories can overlap)
            for external_path in self.dir_states[path_bytes].externals_paths:
                self.remove_external_path(
                    external_path,
                    root_path=path_bytes,
                    remove_subpaths=False,
                    force=True,
                )

        if os.path.isdir(fullpath):
            # versioned and external paths can overlap so we need to iterate on
            # all subpaths to check which ones to remove, paths are iterated in
            # a bottom-up manner to ensure all related dir states are removed
            for root, dirs, files in os.walk(fullpath, topdown=False):
                for p in chain(dirs, files):
                    full_repo_path = os.path.join(root, p)
                    repo_path = full_repo_path.replace(self.rootpath + b"/", b"")
                    if repo_path not in self.editor.external_paths:
                        self.remove_child(repo_path)

        if path_bytes not in self.editor.external_paths:
            self.remove_child(path_bytes)

    def close(self):
        """Function called when we finish processing a repository.

        SVN external definitions are processed by it.
        """
        if self.editor.debug:
            logger.debug("Closing directory %s", self.path)

        prev_externals = self.dir_states[self.path].externals

        if self.externals:
            # externals definition list might have changed in the current replayed
            # revision, we need to determine if some were removed and delete the
            # associated paths
            externals = self.externals
            prev_externals_set = {
                (external.path, external.url, external.revision, external.peg_revision)
                for path in prev_externals.keys()
                for external in prev_externals[path]
            }
            externals_set = {
                (external.path, external.url, external.revision, external.peg_revision)
                for path in externals.keys()
                for external in externals[path]
            }
            old_externals = prev_externals_set - externals_set
            for path, _, _, _ in old_externals:
                self.remove_external_path(os.fsencode(path))
                if path in externals and externals[path]:
                    # case where two externals were previously targeting the same path
                    # and one was removed, export again the remaining one in case its
                    # content changed
                    self.process_external(path, externals[path][0], force=True)
        else:
            # some external paths might have been removed in the current replayed
            # revision by a delete operation on an overlapping versioned path so we
            # need to restore them
            externals = prev_externals

        # For each external, try to export it in reconstructed filesystem
        for path, externals_def in externals.items():
            for i, external in enumerate(externals_def):
                self.process_external(
                    path,
                    external,
                    remove_target_path=i == 0,
                )

        # backup externals in directory state
        if self.externals:
            self.dir_states[self.path].externals = self.externals

        # do operations below only when closing the root directory
        if self.path == b"":
            self.svnrepo.has_relative_externals = any(
                relative_url
                for (_, relative_url) in self.editor.valid_externals.values()
            )

            self.svnrepo.has_recursive_externals = any(
                is_recursive_external(
                    self.svnrepo.origin_url,
                    os.fsdecode(path),
                    external_path,
                    external.url,
                )
                for path, dir_state in self.dir_states.items()
                for external_path in dir_state.externals.keys()
                for external in dir_state.externals[external_path]
            )
            if self.svnrepo.has_recursive_externals:
                # If the repository has recursive externals, we stop processing
                # externals and remove those already exported,
                # We will then ignore externals when exporting the revision to
                # check for divergence with the reconstructed filesystem.
                for external_path in list(self.editor.external_paths):
                    self.remove_external_path(external_path, force=True)

    def process_external(
        self,
        path: str,
        external: ExternalDefinition,
        remove_target_path: bool = True,
        force: bool = False,
    ) -> None:
        dest_path = os.fsencode(path)
        dest_fullpath = os.path.join(self.path, dest_path)
        prev_externals = self.dir_states[self.path].externals

        if (
            path in prev_externals
            and external in prev_externals[path]
            and dest_fullpath in self.directory
            and not force
        ):
            # external already exported, nothing to do
            return

        if is_recursive_external(
            self.svnrepo.origin_url, os.fsdecode(self.path), path, external.url
        ):
            # recursive external, skip it
            return

        logger.debug(
            "Exporting external %s%s%s to path %s",
            external.url,
            f" at revision {external.revision}" if external.revision else "",
            (
                f" and peg revision {external.peg_revision}"
                if external.peg_revision
                else ""
            ),
            dest_fullpath,
        )

        if external not in self.editor.externals_cache or force:
            try:
                # try to export external in a temporary path, destination path could
                # be versioned and must be overridden only if the external URL is
                # still valid
                temp_dir = os.fsencode(
                    tempfile.mkdtemp(dir=self.editor.externals_cache_dir)
                )
                temp_path = os.path.join(temp_dir, dest_path)
                os.makedirs(b"/".join(temp_path.split(b"/")[:-1]), exist_ok=True)
                if (
                    external.url,
                    external.revision,
                    external.peg_revision,
                    external.legacy_format,
                ) not in self.editor.dead_externals:
                    url = external.url.rstrip("/")
                    origin_url = self.svnrepo.origin_url.rstrip("/")
                    if (
                        url.startswith(origin_url + "/")
                        and not self.svnrepo.has_relative_externals
                    ):
                        url = url.replace(origin_url, self.svnrepo.remote_url)

                    peg_revision = external.peg_revision
                    if external.legacy_format and peg_revision is None:
                        peg_revision = external.revision

                    self.svnrepo.export(
                        url,
                        to=temp_path,
                        rev=external.revision,
                        peg_rev=peg_revision,
                        ignore_keywords=True,
                    )
                    self.editor.externals_cache[external] = temp_path

            except SubversionException as se:
                # external no longer available (404)
                logger.debug(se)
                self.editor.dead_externals.add(
                    (
                        external.url,
                        external.revision,
                        external.peg_revision,
                        external.legacy_format,
                    )
                )

        else:
            temp_path = self.editor.externals_cache[external]

        # subversion export will always create the subdirectories of the external
        # path regardless the validity of the remote URL
        dest_path_split = dest_path.split(b"/")
        current_path = self.path
        self.add_directory(os.fsdecode(current_path))
        for subpath in dest_path_split[:-1]:
            current_path = os.path.join(current_path, subpath)
            self.add_directory(os.fsdecode(current_path))

        if os.path.exists(temp_path):
            # external successfully exported

            if remove_target_path:
                # remove previous path in from_disk model
                self.remove_external_path(dest_path, remove_subpaths=False)

            # mark external as valid
            self.editor.valid_externals[dest_fullpath] = (
                external.url,
                external.relative_url,
            )

            # copy exported path to reconstructed filesystem
            fullpath = os.path.join(self.rootpath, dest_fullpath)

            if os.path.isfile(temp_path):
                if os.path.islink(fullpath):
                    # remove destination file if it is a link
                    os.remove(fullpath)
                shutil.copy(os.fsdecode(temp_path), os.fsdecode(fullpath))
                self.directory[dest_fullpath] = from_disk.Content.from_file(
                    path=fullpath, max_content_length=self.max_content_size
                )
            else:
                self.add_directory(os.fsdecode(dest_fullpath))

                shutil.copytree(
                    os.fsdecode(temp_path),
                    os.fsdecode(fullpath),
                    symlinks=True,
                    dirs_exist_ok=True,
                )

                self.directory[dest_fullpath] = from_disk.Directory.from_disk(
                    path=fullpath, max_content_length=self.max_content_size
                )

            # ensure to not count same external paths multiple times
            if path not in prev_externals or external not in prev_externals[path]:
                # update set of external paths reachable from the directory
                external_paths = set()
                dest_path_part = dest_path.split(b"/")
                for i in range(1, len(dest_path_part) + 1):
                    external_paths.add(b"/".join(dest_path_part[:i]))

                for root, dirs, files in os.walk(temp_path):
                    external_paths.update(
                        [
                            os.path.join(
                                dest_path,
                                os.path.join(root, p)
                                .replace(temp_path, b"")
                                .strip(b"/"),
                            )
                            for p in chain(dirs, files)
                        ]
                    )

                self.dir_states[self.path].externals_paths.update(external_paths)

                for external_path in external_paths:
                    self.editor.external_paths.add(
                        os.path.join(self.path, external_path)
                    )

            # ensure hash update for the directory with externals set
            self.directory[self.path].update_hash(force=True)

    def remove_external_path(
        self,
        external_path: bytes,
        remove_subpaths: bool = True,
        force: bool = False,
        root_path: Optional[bytes] = None,
    ) -> None:
        """Remove a previously exported SVN external path from
        the reconstructed filesystem.
        """
        path = root_path if root_path else self.path
        fullpath = os.path.join(path, external_path)

        if self.editor.debug:
            logger.debug("Removing external path %s", fullpath)

        can_remove_external = True
        subpath_split = fullpath.split(b"/")[:-1]
        # check there is no overlapping external set in ancestor directories
        # and mark current external not to be removed if it is the case
        for i in reversed(range(1, len(subpath_split))):
            subpath = b"/".join(subpath_split[0:i])
            subdir_state = self.editor.dir_states.get(subpath)
            if subdir_state and fullpath in {
                os.path.join(subpath, ext_path)
                for ext_path in subdir_state.externals_paths
            }:
                can_remove_external = False
                break

        if force or can_remove_external:
            self.remove_child(fullpath)
            self.editor.external_paths.discard(fullpath)
            self.editor.valid_externals.pop(fullpath, None)
            for path in list(self.editor.external_paths):
                if path.startswith(fullpath + b"/"):
                    self.editor.external_paths.remove(path)

        if remove_subpaths:
            for i in reversed(range(1, len(subpath_split) + 1)):
                # delete external sub-directory only if it is not versioned
                subpath = b"/".join(subpath_split[0:i])
                try:
                    self.svnrepo.info(
                        svn_urljoin(self.svnrepo.repos_root_url, os.fsdecode(subpath)),
                        peg_revision=self.editor.revnum,
                        revision=self.editor.revnum,
                    )
                except SubversionException:
                    self.remove_child(subpath)
                else:
                    break

        try:
            # externals can overlap with versioned files so we must restore
            # them after removing the path above
            dest_path = os.path.join(self.rootpath, fullpath)
            url = svn_urljoin(self.svnrepo.repos_root_url, os.fsdecode(fullpath))
            self.svnrepo.export(
                url,
                to=dest_path,
                peg_rev=self.editor.revnum,
                ignore_keywords=True,
                remove_dest_path=False,
            )
            if os.path.isfile(dest_path) or os.path.islink(dest_path):
                self.directory[fullpath] = from_disk.Content.from_file(
                    path=dest_path, max_content_length=self.max_content_size
                )
            else:
                self.directory[fullpath] = from_disk.Directory.from_disk(
                    path=dest_path, max_content_length=self.max_content_size
                )
        except SubversionException:
            pass


class Editor:
    """Editor in charge of replaying svn events and computing objects
    along.

    This implementation accounts for empty folder during hash
    computations.

    """

    def __init__(
        self,
        rootpath: bytes,
        directory: from_disk.Directory,
        svnrepo: SvnRepo,
        temp_dir: str,
        max_content_size: Optional[int],
        debug: bool = False,
    ):
        self.rootpath = rootpath
        self.directory = directory
        self.dir_states: Dict[bytes, DirState] = defaultdict(DirState)
        self.external_paths: Set[bytes] = set()
        self.valid_externals: Dict[bytes, Tuple[str, bool]] = {}
        self.dead_externals: Set[Tuple[str, Optional[int], Optional[int], bool]] = set()
        self.externals_cache_dir = tempfile.mkdtemp(dir=temp_dir)
        self.externals_cache: Dict[ExternalDefinition, bytes] = {}
        self.svnrepo = svnrepo
        self.revnum = -1
        self.debug = debug
        self.max_content_size = max_content_size

    def set_target_revision(self, revnum) -> None:
        self.revnum = revnum

    def abort(self) -> None:
        pass

    def close(self) -> None:
        pass

    def open_root(self, base_revnum: int) -> DirEditor:
        return DirEditor(
            self.directory,
            rootpath=self.rootpath,
            path=b"",
            dir_states=self.dir_states,
            svnrepo=self.svnrepo,
            max_content_size=self.max_content_size,
        )


class Replay:
    """Replay class."""

    def __init__(
        self,
        conn: RemoteAccess,
        rootpath: bytes,
        svnrepo: SvnRepo,
        temp_dir: str,
        directory: Optional[from_disk.Directory] = None,
        debug: bool = False,
        max_content_size: Optional[int] = None,
    ):
        self.conn = conn
        self.rootpath = rootpath
        if directory is None:
            directory = from_disk.Directory()
        self.directory = directory
        self.editor = Editor(
            rootpath=rootpath,
            directory=directory,
            svnrepo=svnrepo,
            temp_dir=temp_dir,
            debug=debug,
            max_content_size=max_content_size,
        )

    def replay(self, rev: int, low_water_mark: int) -> from_disk.Directory:
        """Replay svn actions between rev and rev+1.

        This method updates in place the self.editor.directory, as well as the
        filesystem.

        Returns:
           The updated root directory

        """
        codecs.register_error("strict", _ra_codecs_error_handler)
        self.conn.replay(rev, low_water_mark, self.editor)
        codecs.register_error("strict", codecs.strict_errors)
        return self.editor.directory

    def compute_objects(
        self, rev: int, low_water_mark: int
    ) -> Tuple[List[Content], List[SkippedContent], List[Directory]]:
        """Compute objects added or modified at revisions rev.
        Expects the state to be at previous revision's objects.

        Args:
            rev: The revision to start the replay from.

        Returns:
            The updated objects between rev and rev+1. Beware that this
            mutates the filesystem at rootpath accordingly.

        """
        self.replay(rev, low_water_mark)

        contents: List[Content] = []
        skipped_contents: List[SkippedContent] = []
        directories: List[Directory] = []

        for obj_node in self.directory.collect():
            obj = obj_node.to_model()  # type: ignore
            obj_type = obj.object_type
            if obj_type == Content.object_type:
                contents.append(obj.with_data())
            elif obj_type == SkippedContent.object_type:
                skipped_contents.append(obj)
            elif obj_type == Directory.object_type:
                directories.append(obj)
            else:
                assert False, obj_type

        return contents, skipped_contents, directories
