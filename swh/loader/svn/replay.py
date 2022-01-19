# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Remote Access client to svn server.

"""

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

import click
from subvertpy import SubversionException, delta, properties
from subvertpy.ra import Auth, RemoteAccess, get_username_provider

from swh.model import from_disk, hashutil
from swh.model.from_disk import DiskBackedContent
from swh.model.model import Content, Directory, SkippedContent

if TYPE_CHECKING:
    from swh.loader.svn.svn import SvnRepo

from swh.loader.svn.utils import parse_external_definition, svn_urljoin

_eol_style = {"native": b"\n", "CRLF": b"\r\n", "LF": b"\n", "CR": b"\r"}

logger = logging.getLogger(__name__)


def _normalize_line_endings(lines: bytes, eol_style: str = "native") -> bytes:
    r"""Normalize line endings to unix (\\n), windows (\\r\\n) or mac (\\r).

    Args:
        lines: The lines to normalize

        eol_style: The line ending format as defined for
            svn:eol-style property. Acceptable values are 'native',
            'CRLF', 'LF' and 'CR'

    Returns:
        Lines with endings normalized
    """
    if eol_style in _eol_style:
        lines = lines.replace(_eol_style["CRLF"], _eol_style["LF"]).replace(
            _eol_style["CR"], _eol_style["LF"]
        )
        if _eol_style[eol_style] != _eol_style["LF"]:
            lines = lines.replace(_eol_style["LF"], _eol_style[eol_style])

    return lines


def apply_txdelta_handler(
    sbuf: bytes, target_stream: BinaryIO
) -> Callable[[Any, bytes, BinaryIO], None]:
    """Return a function that can be called repeatedly with txdelta windows.
    When done, closes the target_stream.

    Adapted from subvertpy.delta.apply_txdelta_handler to close the
    stream when done.

    Args:
        sbuf: Source buffer
        target_stream: Target stream to write to.

    Returns:
        Function to be called to apply txdelta windows

    """

    def apply_window(
        window: Any, sbuf: bytes = sbuf, target_stream: BinaryIO = target_stream
    ):
        if window is None:
            target_stream.close()
            return  # Last call
        patch = delta.apply_txdelta_window(sbuf, window)
        target_stream.write(patch)

    return apply_window


def read_svn_link(data: bytes) -> Tuple[bytes, bytes]:
    """Read the svn link's content.

    Args:
        data: svn link's raw content

    Returns:
        The tuple of (filetype, destination path)

    """
    split_byte = b" "
    first_line = data.split(b"\n")[0]
    filetype, *src = first_line.split(split_byte)
    target = split_byte.join(src)
    return filetype, target


def is_file_an_svnlink_p(fullpath: bytes) -> Tuple[bool, bytes]:
    """Determine if a filepath is an svnlink or something else.

    Args:
        fullpath: Full path to the potential symlink to check

    Returns:
        Tuple containing a boolean value to determine if it's indeed a symlink
        (as per svn) and the link target.

    """
    if os.path.islink(fullpath):
        return False, b""
    with open(fullpath, "rb") as f:
        filetype, src = read_svn_link(f.read())
        return filetype == b"link", src


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


@dataclass
class FileState:
    """Persists some file states (eg. end of lines style) across revisions while
    replaying them."""

    eol_style: Optional[str] = None
    """EOL state check mess"""

    svn_special_path_non_link_data: Optional[bytes] = None
    """keep track of non link file content with svn:special property set"""

    # default value: 0, 1: set the flag, 2: remove the exec flag
    executable: int = DEFAULT_FLAG
    """keep track if file is executable when setting svn:executable property"""

    link: bool = False
    """keep track if file is a svn link when setting svn:special property"""


class FileEditor:
    """File Editor in charge of updating file on disk and memory objects.

    """

    __slots__ = [
        "directory",
        "path",
        "fullpath",
        "executable",
        "link",
        "state",
        "svnrepo",
        "editor",
    ]

    def __init__(
        self,
        directory: from_disk.Directory,
        rootpath: bytes,
        path: bytes,
        state: FileState,
        svnrepo: SvnRepo,
    ):
        self.directory = directory
        self.path = path
        self.fullpath = os.path.join(rootpath, path)
        self.state = state
        self.svnrepo = svnrepo
        self.editor = svnrepo.swhreplay.editor

        self.editor.modified_paths.add(path)

    def change_prop(self, key: str, value: str) -> None:
        if key == properties.PROP_EXECUTABLE:
            if value is None:  # bit flip off
                self.state.executable = NOEXEC_FLAG
            else:
                self.state.executable = EXEC_FLAG
        elif key == properties.PROP_SPECIAL:
            # Possibly a symbolic link. We cannot check further at
            # that moment though, patch(s) not being applied yet
            self.state.link = value is not None
        elif key == SVN_PROPERTY_EOL:
            # backup end of line style for file
            self.state.eol_style = value

    def __make_symlink(self, src: bytes) -> None:
        """Convert the svnlink to a symlink on disk.

        This function expects self.fullpath to be a svn link.

        Args:
            src: Path to the link's source

        Return:
            tuple: The svnlink's data tuple:

                - type (should be only 'link')
                - <path-to-src>

        """
        os.remove(self.fullpath)
        os.symlink(src=src, dst=self.fullpath)

    def __make_svnlink(self) -> bytes:
        """Convert the symlink to a svnlink on disk.

        Return:
            The symlink's svnlink data (``b'type <path-to-src>'``)

        """
        # we replace the symlink by a svnlink
        # to be able to patch the file on future commits
        src = os.readlink(self.fullpath)
        os.remove(self.fullpath)
        sbuf = b"link " + src
        with open(self.fullpath, "wb") as f:
            f.write(sbuf)
        return sbuf

    def apply_textdelta(self, base_checksum) -> Callable[[Any, bytes, BinaryIO], None]:
        # if the filepath matches an external, do not apply local patch
        if self.path in self.editor.external_paths:
            return lambda *args: None

        if os.path.lexists(self.fullpath):
            if os.path.islink(self.fullpath):
                # svn does not deal with symlink so we transform into
                # real svn symlink for potential patching in later
                # commits
                sbuf = self.__make_svnlink()
                self.state.link = True
            else:
                with open(self.fullpath, "rb") as f:
                    sbuf = f.read()
        else:
            sbuf = b""

        t = open(self.fullpath, "wb")
        return apply_txdelta_handler(sbuf, target_stream=t)

    def close(self) -> None:
        """When done with the file, this is called.

        So the file exists and is updated, we can:

        - adapt accordingly its execution flag if any
        - compute the objects' checksums
        - replace the svnlink with a real symlink (for disk
          computation purposes)

        """

        if self.state.link:
            # can only check now that the link is a real one
            # since patch has been applied
            is_link, src = is_file_an_svnlink_p(self.fullpath)
            if is_link:
                self.__make_symlink(src)
            elif not os.path.isdir(self.fullpath):  # not a real link ...
                # when a file with the svn:special property set is not a svn link,
                # the svn export operation might extract a truncated version of it
                # if it is a binary file, so ensure to produce the same file as the
                # export operation.
                with open(self.fullpath, "rb") as f:
                    content = f.read()
                self.svnrepo.client.export(
                    os.path.join(self.svnrepo.remote_url.encode(), self.path),
                    to=self.fullpath,
                    rev=self.editor.revnum,
                    ignore_keywords=True,
                    overwrite=True,
                )
                with open(self.fullpath, "rb") as f:
                    exported_data = f.read()
                    if exported_data != content:
                        # keep track of original file content in order to restore
                        # it if the svn:special property gets unset in another revision
                        self.state.svn_special_path_non_link_data = content
        elif os.path.islink(self.fullpath):
            # path was a symbolic link in previous revision but got the property
            # svn:special unset in current one, revert its content to svn link format
            self.__make_svnlink()
        elif self.state.svn_special_path_non_link_data is not None:
            # path was a non link file with the svn:special property previously set
            # and got truncated on export, restore its original content
            with open(self.fullpath, "wb") as f:
                f.write(self.state.svn_special_path_non_link_data)
                self.state.svn_special_path_non_link_data = None

        is_link = os.path.islink(self.fullpath)
        if not is_link:  # if a link, do nothing regarding flag
            if self.state.executable == EXEC_FLAG:
                os.chmod(self.fullpath, 0o755)
            elif self.state.executable == NOEXEC_FLAG:
                os.chmod(self.fullpath, 0o644)

        # And now compute file's checksums
        if self.state.eol_style and not is_link:
            # ensure to normalize line endings as defined by svn:eol-style
            # property to get the same file checksum as after an export
            # or checkout operation with subversion
            with open(self.fullpath, "rb") as f:
                data = f.read()
                data = _normalize_line_endings(data, self.state.eol_style)
                mode = os.lstat(self.fullpath).st_mode
                self.directory[self.path] = from_disk.Content.from_bytes(
                    mode=mode, data=data
                )
        else:
            self.directory[self.path] = from_disk.Content.from_file(path=self.fullpath)


@dataclass
class DirState:
    """Persists some directory states (eg. externals) across revisions while
    replaying them."""

    externals: Dict[str, Tuple[str, Optional[int]]] = field(default_factory=dict)


class DirEditor:
    """Directory Editor in charge of updating directory hashes computation.

    This implementation includes empty folder in the hash computation.

    """

    __slots__ = [
        "directory",
        "rootpath",
        "path",
        "file_states",
        "dir_states",
        "svnrepo",
        "editor",
        "externals",
    ]

    def __init__(
        self,
        directory: from_disk.Directory,
        rootpath: bytes,
        path: bytes,
        file_states: Dict[bytes, FileState],
        dir_states: Dict[bytes, DirState],
        svnrepo: SvnRepo,
    ):
        self.directory = directory
        self.rootpath = rootpath
        self.path = path
        # build directory on init
        os.makedirs(rootpath, exist_ok=True)
        self.file_states = file_states
        self.dir_states = dir_states
        self.svnrepo = svnrepo
        self.editor = svnrepo.swhreplay.editor
        self.externals: Dict[str, Tuple[str, Optional[int], bool]] = {}

        # repository root dir has empty path
        if path:
            self.editor.modified_paths.add(path)

    def remove_child(self, path: bytes) -> None:
        """Remove a path from the current objects.

        The path can be resolved as link, file or directory.

        This function takes also care of removing the link between the
        child and the parent.

        Args:
            path: to remove from the current objects.

        """
        try:
            entry_removed = self.directory[path]
        except KeyError:
            entry_removed = None
        else:
            del self.directory[path]
            fpath = os.path.join(self.rootpath, path)
            if isinstance(entry_removed, from_disk.Directory):
                shutil.rmtree(fpath)
            else:
                os.remove(fpath)

        # when deleting a directory ensure to remove any svn property for the
        # file it contains as they can be added again later in another revision
        # without the same property set
        fullpath = os.path.join(self.rootpath, path)
        for state_path in list(self.file_states):
            if state_path.startswith(fullpath + b"/"):
                del self.file_states[state_path]

        self.editor.modified_paths.discard(path)

    def open_directory(self, path: str, *args) -> DirEditor:
        """Updating existing directory.

        """
        return DirEditor(
            self.directory,
            rootpath=self.rootpath,
            path=os.fsencode(path),
            file_states=self.file_states,
            dir_states=self.dir_states,
            svnrepo=self.svnrepo,
        )

    def add_directory(self, path: str, *args) -> DirEditor:
        """Adding a new directory.

        """
        path_bytes = os.fsencode(path)

        os.makedirs(os.path.join(self.rootpath, path_bytes), exist_ok=True)
        if path_bytes and path_bytes not in self.directory:
            self.dir_states[path_bytes] = DirState()
            self.directory[path_bytes] = from_disk.Directory()

        return DirEditor(
            self.directory,
            self.rootpath,
            path_bytes,
            self.file_states,
            self.dir_states,
            svnrepo=self.svnrepo,
        )

    def open_file(self, *args) -> FileEditor:
        """Updating existing file.

        """
        path = os.fsencode(args[0])
        self.directory[path] = from_disk.Content()
        fullpath = os.path.join(self.rootpath, path)
        return FileEditor(
            self.directory,
            rootpath=self.rootpath,
            path=path,
            state=self.file_states[fullpath],
            svnrepo=self.svnrepo,
        )

    def add_file(self, path: str, *args) -> FileEditor:
        """Creating a new file.

        """
        path_bytes = os.fsencode(path)
        self.directory[path_bytes] = from_disk.Content()
        fullpath = os.path.join(self.rootpath, path_bytes)
        self.file_states[fullpath] = FileState()
        return FileEditor(
            self.directory,
            self.rootpath,
            path_bytes,
            state=self.file_states[fullpath],
            svnrepo=self.svnrepo,
        )

    def change_prop(self, key: str, value: str) -> None:
        """Change property callback on directory.

        """
        if key == properties.PROP_EXTERNALS:
            logger.debug(
                "Setting '%s' property with value '%s' on path %s",
                key,
                value,
                self.path,
            )
            self.externals = {}
            if value is not None:
                # externals are set on that directory path, parse and store them
                # for later processing in the close method
                for external in value.split("\n"):
                    external = external.rstrip("\r")
                    # skip empty line or comment
                    if not external or external.startswith("#"):
                        continue
                    (
                        path,
                        external_url,
                        revision,
                        relative_url,
                    ) = parse_external_definition(
                        external, os.fsdecode(self.path), self.svnrepo.origin_url
                    )
                    self.externals[path] = (external_url, revision, relative_url)

            if not self.externals:
                # externals might have been unset on that directory path,
                # remove associated paths from the reconstructed filesystem
                externals = self.dir_states[self.path].externals
                for path in externals.keys():
                    self.remove_external_path(os.fsencode(path))

                self.dir_states[self.path].externals = {}

    def delete_entry(self, path: str, revision: int) -> None:
        """Remove a path.

        """
        path_bytes = os.fsencode(path)
        if path_bytes not in self.editor.external_paths:
            fullpath = os.path.join(self.rootpath, path_bytes)
            self.file_states.pop(fullpath, None)
            self.remove_child(path_bytes)

    def close(self):
        """Function called when we finish processing a repository.

        SVN external definitions are processed by it.
        """

        prev_externals = self.dir_states[self.path].externals

        if self.externals:
            # externals definition list might have changed in the current replayed
            # revision, we need to determine if some were removed and delete the
            # associated paths
            externals = self.externals
            old_externals = set(prev_externals) - set(self.externals)
            for old_external in old_externals:
                self.remove_external_path(os.fsencode(old_external))
        else:
            # some external paths might have been removed in the current replayed
            # revision by a delete operation on an overlapping versioned path so we
            # need to restore them
            externals = prev_externals

        # For each external, try to export it in reconstructed filesystem
        for path, (external_url, revision, relative_url) in externals.items():
            external = (external_url, revision)
            dest_path = os.fsencode(path)
            dest_fullpath = os.path.join(self.path, dest_path)
            if (
                path in prev_externals
                and prev_externals[path] == external
                and dest_fullpath in self.directory
            ):
                # external already exported, nothing to do
                continue

            if external not in self.editor.externals_cache:

                try:
                    # try to export external in a temporary path, destination path could
                    # be versioned and must be overridden only if the external URL is
                    # still valid
                    temp_dir = os.fsencode(
                        tempfile.mkdtemp(dir=self.editor.externals_cache_dir)
                    )
                    temp_path = os.path.join(temp_dir, dest_path)
                    os.makedirs(b"/".join(temp_path.split(b"/")[:-1]), exist_ok=True)
                    if external_url not in self.editor.dead_externals:
                        logger.debug(
                            "Exporting external %s to path %s", external_url, path
                        )
                        self.svnrepo.client.export(
                            external_url.rstrip("/"),
                            to=temp_path,
                            rev=revision,
                            ignore_keywords=True,
                        )
                        self.editor.externals_cache[external] = temp_path
                        self.editor.valid_externals[dest_fullpath] = (
                            external_url,
                            relative_url,
                        )

                except SubversionException as se:
                    # external no longer available (404)
                    logger.debug(se)
                    self.editor.dead_externals.add(external_url)

            else:
                temp_path = self.editor.externals_cache[external]

            # subversion export will always create the subdirectories of the external
            # path regardless the validity of the remote URL
            dest_path_split = dest_path.split(b"/")
            current_path = self.path
            self.add_directory(current_path)
            for subpath in dest_path_split[:-1]:
                current_path = os.path.join(current_path, subpath)
                self.add_directory(current_path)

            if os.path.exists(temp_path):
                # external successfully exported

                # remove previous path in from_disk model
                self.remove_child(dest_fullpath)
                # copy exported path to reconstructed filesystem
                fullpath = os.path.join(self.rootpath, dest_fullpath)

                self.editor.external_paths.add(dest_fullpath)
                self.editor.modified_paths.add(dest_fullpath)
                # update from_disk model and store external paths
                self.editor.external_paths.add(dest_fullpath)
                if os.path.isfile(temp_path):
                    shutil.copy(temp_path, fullpath)
                    self.directory[dest_fullpath] = from_disk.Content.from_file(
                        path=fullpath
                    )
                else:
                    shutil.copytree(temp_path, fullpath, symlinks=True)
                    self.directory[dest_fullpath] = from_disk.Directory.from_disk(
                        path=fullpath
                    )
                    external_paths = set()
                    for root, dirs, files in os.walk(fullpath):
                        external_paths.update(
                            [
                                os.path.join(root.replace(self.rootpath + b"/", b""), p)
                                for p in chain(dirs, files)
                            ]
                        )
                    self.editor.external_paths.update(external_paths)
                    self.editor.modified_paths.update(external_paths)

                # ensure hash update for the directory with externals set
                self.directory[self.path].update_hash(force=True)

        # backup externals in directory state
        if self.externals:
            self.dir_states[self.path].externals = self.externals

        self.svnrepo.has_relative_externals = any(
            [relative_url for (_, relative_url) in self.editor.valid_externals.values()]
        )

    def remove_external_path(self, external_path: bytes) -> None:
        """Remove a previously exported SVN external path from
        the reconstruted filesystem.
        """
        fullpath = os.path.join(self.path, external_path)
        self.remove_child(fullpath)
        self.editor.external_paths.discard(fullpath)
        self.editor.valid_externals.pop(fullpath, None)
        for path in list(self.editor.external_paths):
            if path.startswith(fullpath + b"/"):
                self.editor.external_paths.remove(path)
        subpath_split = external_path.split(b"/")[:-1]
        for i in reversed(range(1, len(subpath_split) + 1)):
            # delete external sub-directory only if it is empty
            subpath = os.path.join(self.path, b"/".join(subpath_split[0:i]))
            if not os.listdir(os.path.join(self.rootpath, subpath)):
                self.remove_child(subpath)
            else:
                break

        try:
            # externals can overlap with versioned files so we must restore
            # them after removing the path above
            dest_path = os.path.join(self.rootpath, fullpath)
            self.svnrepo.client.export(
                svn_urljoin(self.svnrepo.remote_url, os.fsdecode(fullpath)),
                to=dest_path,
                rev=self.editor.revnum,
                ignore_keywords=True,
            )
            if os.path.isfile(dest_path):
                self.directory[fullpath] = from_disk.Content.from_file(path=dest_path)
            else:
                self.directory[fullpath] = from_disk.Directory.from_disk(path=dest_path)
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
    ):
        self.rootpath = rootpath
        self.directory = directory
        self.file_states: Dict[bytes, FileState] = defaultdict(FileState)
        self.dir_states: Dict[bytes, DirState] = defaultdict(DirState)
        self.external_paths: Set[bytes] = set()
        self.valid_externals: Dict[bytes, Tuple[str, bool]] = {}
        self.dead_externals: Set[str] = set()
        self.externals_cache_dir = tempfile.mkdtemp(dir=temp_dir)
        self.externals_cache: Dict[Tuple[str, Optional[int]], str] = {}
        self.svnrepo = svnrepo
        self.revnum = None
        # to store the set of paths added or modified when replaying a revision
        self.modified_paths: Set[bytes] = set()

    def set_target_revision(self, revnum) -> None:
        self.revnum = revnum

    def abort(self) -> None:
        pass

    def close(self) -> None:
        pass

    def open_root(self, base_revnum: int) -> DirEditor:
        # a new revision is being replayed so clear the modified_paths set
        self.modified_paths.clear()
        return DirEditor(
            self.directory,
            rootpath=self.rootpath,
            path=b"",
            file_states=self.file_states,
            dir_states=self.dir_states,
            svnrepo=self.svnrepo,
        )


class Replay:
    """Replay class.
    """

    def __init__(
        self,
        conn: RemoteAccess,
        rootpath: bytes,
        svnrepo: SvnRepo,
        temp_dir: str,
        directory: Optional[from_disk.Directory] = None,
    ):
        self.conn = conn
        self.rootpath = rootpath
        if directory is None:
            directory = from_disk.Directory()
        self.directory = directory
        self.editor = Editor(
            rootpath=rootpath, directory=directory, svnrepo=svnrepo, temp_dir=temp_dir
        )

    def replay(self, rev: int) -> from_disk.Directory:
        """Replay svn actions between rev and rev+1.

        This method updates in place the self.editor.directory, as well as the
        filesystem.

        Returns:
           The updated root directory

        """
        codecs.register_error("strict", _ra_codecs_error_handler)
        self.conn.replay(rev, rev + 1, self.editor)
        codecs.register_error("strict", codecs.strict_errors)
        return self.editor.directory

    def compute_objects(
        self, rev: int
    ) -> Tuple[List[Content], List[SkippedContent], List[Directory]]:
        """Compute objects added or modified at revisions rev.
        Expects the state to be at previous revision's objects.

        Args:
            rev: The revision to start the replay from.

        Returns:
            The updated objects between rev and rev+1. Beware that this
            mutates the filesystem at rootpath accordingly.

        """
        self.replay(rev)

        contents: List[Content] = []
        skipped_contents: List[SkippedContent] = []
        directories: List[Directory] = []

        directories.append(self.editor.directory.to_model())
        for path in self.editor.modified_paths:
            obj = self.directory[path].to_model()
            obj_type = obj.object_type
            if obj_type in (Content.object_type, DiskBackedContent.object_type):
                contents.append(obj.with_data())
            elif obj_type == SkippedContent.object_type:
                skipped_contents.append(obj)
            elif obj_type == Directory.object_type:
                directories.append(obj)

        return contents, skipped_contents, directories


@click.command()
@click.option("--local-url", default="/tmp", help="local svn working copy")
@click.option(
    "--svn-url",
    default="file:///home/storage/svn/repos/pkg-fox",
    help="svn repository's url.",
)
@click.option(
    "--revision-start",
    default=1,
    type=click.INT,
    help="svn repository's starting revision.",
)
@click.option(
    "--revision-end",
    default=-1,
    type=click.INT,
    help="svn repository's ending revision.",
)
@click.option(
    "--debug/--nodebug",
    default=True,
    help="Indicates if the server should run in debug mode.",
)
@click.option(
    "--cleanup/--nocleanup",
    default=True,
    help="Indicates whether to cleanup disk when done or not.",
)
def main(local_url, svn_url, revision_start, revision_end, debug, cleanup):
    """Script to present how to use Replay class.

    """
    conn = RemoteAccess(svn_url.encode("utf-8"), auth=Auth([get_username_provider()]))

    os.makedirs(local_url, exist_ok=True)

    rootpath = tempfile.mkdtemp(
        prefix=local_url, suffix="-" + os.path.basename(svn_url)
    )

    rootpath = os.fsencode(rootpath)

    # Do not go beyond the repository's latest revision
    revision_end_max = conn.get_latest_revnum()
    if revision_end == -1:
        revision_end = revision_end_max

    revision_end = min(revision_end, revision_end_max)

    try:
        replay = Replay(conn, rootpath)

        for rev in range(revision_start, revision_end + 1):
            contents, skipped_contents, directories = replay.compute_objects(rev)
            print(
                "r%s %s (%s new contents, %s new directories)"
                % (
                    rev,
                    hashutil.hash_to_hex(replay.directory.hash),
                    len(contents) + len(skipped_contents),
                    len(directories),
                )
            )

        if debug:
            print("%s" % rootpath.decode("utf-8"))
    finally:
        if cleanup:
            if os.path.exists(rootpath):
                shutil.rmtree(rootpath)


if __name__ == "__main__":
    main()
