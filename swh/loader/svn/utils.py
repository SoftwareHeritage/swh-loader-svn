# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import errno
import logging
import os
import re
import shutil
from subprocess import PIPE, Popen, call, run
import tempfile
from typing import Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse

logger = logging.getLogger(__name__)


class OutputStream:
    """Helper class to read lines from a program output while
    it is running

    Args:
        fileno (int): File descriptor of a program output stream
            opened in text mode
    """

    def __init__(self, fileno):
        self._fileno = fileno
        self._buffer = ""

    def read_lines(self):
        """
        Read available lines from the output stream and return them.

        Returns:
            Tuple[List[str], bool]: A tuple whose first member is the read
                lines and second member a boolean indicating if there are
                still some other lines available to read.
        """
        try:
            output = os.read(self._fileno, 1000).decode()
        except OSError as e:
            if e.errno != errno.EIO:
                raise
            output = ""
        output = output.replace("\r\n", "\n")
        lines = output.split("\n")
        lines[0] = self._buffer + lines[0]

        if output:
            self._buffer = lines[-1]
            return (lines[:-1], True)
        else:
            self._buffer = ""
            if len(lines) == 1 and not lines[0]:
                lines = []
            return (lines, False)


def init_svn_repo_from_dump(
    dump_path: str,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    root_dir: str = "/tmp",
    gzip: bool = False,
    cleanup_dump: bool = True,
    max_rev: int = -1,
) -> Tuple[str, str]:
    """Given a path to a svn dump, initialize an svn repository with the content of said
    dump.

    Args:
        dump_path: The dump to the path
        prefix: optional prefix file name for the working directory
        suffix: optional suffix file name for the working directory
        root_dir: the root directory where the working directory is created
        gzip: Boolean to determine whether we treat the dump as compressed or not.
        cleanup_dump: Whether we want this function call to clean up the dump at the end
            of the repository initialization.

    Raises:
        ValueError in case of failure to run the command to uncompress and load the
        dump.

    Returns:
        A tuple:
        - temporary folder: containing the mounted repository
        - repo_path: path to the mounted repository inside the temporary folder

    """
    project_name = os.path.basename(os.path.dirname(dump_path))
    temp_dir = tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=root_dir)

    try:
        repo_path = os.path.join(temp_dir, project_name)

        # create the repository that will be loaded with the dump
        cmd = ["svnadmin", "create", repo_path]
        r = call(cmd)
        if r != 0:
            raise ValueError(
                "Failed to initialize empty svn repo for %s" % project_name
            )

        read_dump_cmd = ["cat", dump_path]
        if gzip:
            read_dump_cmd = ["gzip", "-dc", dump_path]

        with Popen(read_dump_cmd, stdout=PIPE) as dump:
            # load dump and bypass properties validation as Unicode decoding errors
            # are already handled in loader implementation (see _ra_codecs_error_handler
            # in ra.py)
            cmd = ["svnadmin", "load", "-q", "--bypass-prop-validation"]
            if max_rev > 0:
                cmd.append(f"-r1:{max_rev}")
            cmd.append(repo_path)
            svnadmin_load = run(cmd, stdin=dump.stdout, capture_output=True, text=True)
            if svnadmin_load.returncode != 0:
                if max_rev > 0:
                    # if max_rev is specified, we might have a truncated dump due to
                    # an error when executing svnrdump, check if max_rev have been
                    # loaded and continue loading process if it is the case
                    svnadmin_info = run(
                        ["svnadmin", "info", repo_path], capture_output=True, text=True
                    )
                    if f"Revisions: {max_rev}\n" in svnadmin_info.stdout:
                        return temp_dir, repo_path
                raise ValueError(
                    f"Failed to mount the svn dump for project {project_name}\n"
                    + svnadmin_load.stderr
                )
            return temp_dir, repo_path
    except Exception as e:
        shutil.rmtree(temp_dir)
        raise e
    finally:
        if cleanup_dump:
            try:
                # At this time, the temporary svn repository is mounted from the dump or
                # the svn repository failed to mount. Either way, we can drop the dump.
                os.remove(dump_path)
                assert not os.path.exists(dump_path)
            except OSError as e:
                logger.warn("Failure to remove the dump %s: %s", dump_path, e)


def init_svn_repo_from_archive_dump(
    archive_path: str,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    root_dir: str = "/tmp",
    cleanup_dump: bool = True,
) -> Tuple[str, str]:
    """Given a path to an archive containing an svn dump, initializes an svn repository
    with the content of the uncompressed dump.

    Args:
        archive_path: The archive svn dump path
        prefix: optional prefix file name for the working directory
        suffix: optional suffix file name for the working directory
        root_dir: the root directory where the working directory is created
        gzip: Boolean to determine whether we treat the dump as compressed or not.
        cleanup_dump: Whether we want this function call to clean up the dump at the end
            of the repository initialization.
    Raises:
        ValueError in case of failure to run the command to uncompress
        and load the dump.

    Returns:
        A tuple:
        - temporary folder: containing the mounted repository
        - repo_path: path to the mounted repository inside the
            temporary folder

    """
    return init_svn_repo_from_dump(
        archive_path,
        prefix=prefix,
        suffix=suffix,
        root_dir=root_dir,
        gzip=True,
        cleanup_dump=cleanup_dump,
    )


def svn_urljoin(base_url: str, *args) -> str:
    """Join a base URL and a list of paths in a SVN way.

    For instance:

        - svn_urljoin("http://example.org", "foo", "bar")
            will return "https://example.org/foo/bar

        - svn_urljoin("http://example.org/foo", "../bar")
            will return "https://example.org/bar

    Args:
        base_url: Base URL to join paths with
        args: path components

    Returns:
        The joined URL

    """
    parsed_url = urlparse(base_url)
    path = os.path.abspath(
        os.path.join(parsed_url.path or "/", *[arg.strip("/") for arg in args])
    )
    return f"{parsed_url.scheme}://{parsed_url.netloc}{path}"


def parse_external_definition(
    external: str, dir_path: str, repo_url: str
) -> Tuple[str, str, Optional[int], bool]:
    """Parse a subversion external definition.

    Args:
        external: an external definition, extracted from the lines split of a
            svn:externals property value
        dir_path: The path of the directory in the subversion repository where
            the svn:externals property was set
        repo_url: URL of the subversion repository

    Returns:
        A tuple with the following members:

            - path relative to dir_path where the external should be exported
            - URL of the external to export
            - optional revision of the external to export
            - boolean indicating if the external URL is relative to the repository
              URL and targets a path not in the repository

    """
    path = ""
    external_url = ""
    revision = None
    relative_url = False
    prev_part = None
    # turn multiple spaces into a single one and split on space
    for external_part in external.split():
        if prev_part == "-r":
            # parse revision in the form "-r XXX"
            revision = int(external_part)
        elif external_part.startswith("-r") and external_part != "-r":
            # parse revision in the form "-rXXX"
            revision = int(external_part[2:])
        elif external_part.startswith("^/"):
            # URL relative to the root of the repository in which the svn:externals
            # property is versioned
            external_url = svn_urljoin(repo_url, external_part[2:])
            relative_url = not external_url.startswith(repo_url)
        elif external_part.startswith("//"):
            # URL relative to the scheme of the URL of the directory on which the
            # svn:externals property is set
            scheme = urlparse(repo_url).scheme
            external_url = f"{scheme}:{external_part}"
            relative_url = not external_url.startswith(repo_url)
        elif external_part.startswith("/"):
            # URL relative to the root URL of the server on which the svn:externals
            # property is versioned
            parsed_url = urlparse(repo_url)
            root_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            external_url = svn_urljoin(root_url, external_part)
            relative_url = not external_url.startswith(repo_url)
        elif external_part.startswith("../"):
            # URL relative to the URL of the directory on which the svn:externals
            # property is set
            external_url = svn_urljoin(repo_url, dir_path, external_part)
            relative_url = not external_url.startswith(repo_url)
        elif re.match(r"^.*:*//.*", external_part):
            # absolute external URL
            external_url = external_part
        # subversion >= 1.6 added a quoting and escape mechanism to the syntax so
        # that the path of the external working copy may contain whitespace.
        elif external_part.startswith('\\"'):
            external_split = external.split('\\"')
            path = [
                e.replace("\\ ", " ")
                for e in external_split
                if e.startswith(external_part[2:])
            ][0]
            path = f'"{path}"'
        elif external_part.endswith('\\"'):
            continue
        elif external_part.startswith('"'):
            external_split = external.split('"')
            path_prefix = external_part.strip('"')
            path = next(iter([e for e in external_split if e.startswith(path_prefix)]))
        elif external_part.endswith('"'):
            continue
        elif not external_part.startswith("\\") and external_part != "-r":
            # path of the external relative to dir_path
            path = external_part.replace("\\\\", "\\")
            if path == external_part:
                path = external_part.replace("\\", "")
            if path.startswith("./"):
                path = path.replace("./", "", 1)
        prev_part = external_part
    if "@" in external_url:
        # try to extract revision number if external URL is in the form
        # http://svn.example.org/repos/test/path@XXX
        url, revision_s = external_url.rsplit("@", maxsplit=1)
        try:
            # ensure revision_s can be parsed to int
            rev = int(revision_s)
            # -r XXX takes priority over <svn_url>@XXX
            revision = revision or rev
            external_url = url
        except ValueError:
            # handle URL like http://user@svn.example.org/
            pass

    if not external_url or not path:
        raise ValueError(f"Failed to parse external definition '{external}'")

    return (path.rstrip("/"), external_url, revision, relative_url)


def is_recursive_external(
    origin_url: str, dir_path: str, external_path: str, external_url: str
) -> bool:
    """
    Check if an external definition can lead to a recursive subversion export
    operation (https://issues.apache.org/jira/browse/SVN-1703).

    Args:
        origin_url: repository URL
        dir_path: path of the directory where external is defined
        external_path: path of the external relative to the directory
        external_url: external URL

    Returns:
        Whether the external definition is recursive
    """
    assert external_url
    parsed_origin_url = urlparse(origin_url)
    parsed_external_url = urlparse(external_url)
    external_url = urlunparse(
        parsed_external_url._replace(scheme=parsed_origin_url.scheme)
    )
    return svn_urljoin(origin_url, quote(dir_path), quote(external_path)).startswith(
        external_url
    )
