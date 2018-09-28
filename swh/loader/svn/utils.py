# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile
import shutil

from dateutil import parser
from subprocess import PIPE, Popen, call


def strdate_to_timestamp(strdate):
    """Convert a string date to an int timestamp.

    Args:
        strdate: A string representing a date with format like
        'YYYY-mm-DDTHH:MM:SS.800722Z'

    Returns:
        A couple of integers: seconds, microseconds

    """
    if strdate:
        dt = parser.parse(strdate)
        ts = {
            'seconds': int(dt.timestamp()),
            'microseconds': dt.microsecond,
        }
    else:  # epoch
        ts = {'seconds': 0, 'microseconds': 0}
    return ts


def init_svn_repo_from_dump(dump_path, prefix=None, suffix=None,
                            root_dir='/tmp', gzip=False):
    """Given a path to a svn dump.
    Initialize an svn repository with the content of said dump.

    Returns:
        A tuple:
        - temporary folder (str): containing the mounted repository
        - repo_path (str): path to the mounted repository inside the
                           temporary folder

    Raises:
        ValueError in case of failure to run the command to uncompress
        and load the dump.

    """
    project_name = os.path.basename(os.path.dirname(dump_path))
    temp_dir = tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=root_dir)

    try:
        repo_path = os.path.join(temp_dir, project_name)

        # create the repository that will be loaded with the dump
        cmd = ['svnadmin', 'create', repo_path]
        r = call(cmd)
        if r != 0:
            raise ValueError(
                'Failed to initialize empty svn repo for %s' %
                project_name)

        read_dump_cmd = ['cat', dump_path]
        if gzip:
            read_dump_cmd = ['gzip', '-dc', dump_path]

        with Popen(read_dump_cmd, stdout=PIPE) as dump:
            cmd = ['svnadmin', 'load', '-q', repo_path]
            r = call(cmd, stdin=dump.stdout)
            if r != 0:
                raise ValueError(
                    'Failed to mount the svn dump for project %s' %
                    project_name)
            return temp_dir, repo_path
    except Exception as e:
        shutil.rmtree(temp_dir)
        raise e


def init_svn_repo_from_archive_dump(archive_path, prefix=None, suffix=None,
                                    root_dir='/tmp'):
    """Given a path to an archive containing an svn dump.
    Initialize an svn repository with the content of said dump.

    Returns:
        A tuple:
        - temporary folder (str): containing the mounted repository
        - repo_path (str): path to the mounted repository inside the
                           temporary folder

    Raises:
        ValueError in case of failure to run the command to uncompress
        and load the dump.

    """
    return init_svn_repo_from_dump(archive_path, prefix=prefix, suffix=suffix,
                                   root_dir=root_dir, gzip=True)
