# Copyright (C) 2016-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import errno
import logging
import os
import shutil
from subprocess import PIPE, Popen, call
import tempfile
from typing import Tuple

from dateutil import parser

from swh.model.model import Optional, Timestamp

logger = logging.getLogger(__name__)


def strdate_to_timestamp(strdate: Optional[str]) -> Timestamp:
    """Convert a string date to an int timestamp.

    Args:
        strdate: A string representing a date with format like
        'YYYY-mm-DDTHH:MM:SS.800722Z'

    Returns:
        A couple of integers: seconds, microseconds

    """
    if strdate:
        # TODO: Migrate to iso8601 if possible
        dt = parser.parse(strdate)
        ts = {
            "seconds": int(dt.timestamp()),
            "microseconds": dt.microsecond,
        }
    else:  # epoch
        ts = {"seconds": 0, "microseconds": 0}
    return Timestamp.from_dict(ts)


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
            cmd = ["svnadmin", "load", "-q", "--bypass-prop-validation", repo_path]
            r = call(cmd, stdin=dump.stdout)
            if r != 0:
                raise ValueError(
                    "Failed to mount the svn dump for project %s" % project_name
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
