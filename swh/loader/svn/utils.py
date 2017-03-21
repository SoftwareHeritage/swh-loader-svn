# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile
import shutil

from dateutil import parser
from subprocess import PIPE, Popen, call

from swh.model import git


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


def convert_hashes_with_relative_path(hashes, rootpath):
    """A function to ease the transformation of absolute path to relative ones.

    This is an implementation detail:
    - swh.loader.svn.ra compute hashes and store keys with relative paths
    - swh.model.git compute hashes and store keys with full paths

    """
    if rootpath.endswith(b'/'):
        rootpath = rootpath[:-1]

    root_value = hashes.pop(rootpath)

    if not rootpath.endswith(b'/'):
        rootpath = rootpath + b'/'

    def _replace_slash(s, rootpath=rootpath):
        return s.replace(rootpath, b'')

    def _update_children(children):
        return set((_replace_slash(c) for c in children))

    h = {
        b'': {
            'checksums': root_value['checksums'],
            'children': _update_children(root_value['children'])
        }
    }
    for path, v in hashes.items():
        p = _replace_slash(path)
        if 'children' in v:
            v['children'] = _update_children(v['children'])

        h[p] = v

    return h


def hashtree(path, ignore_empty_folder=False, ignore=None):
    """Given a path and options, compute the hash's upper tree.

    This is not for production use.
    It's merely a helper function used mainly in bin/swh-hashtree

    Args:
        - path: The path to hash
        - ignore_empty_folder: An option to ignore empty folder
        - ignore: An option to ignore patterns in directory names.

    Returns:
        The path's checksums respecting the options passed as
        parameters.

    """
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise ValueError('%s should be a directory!' % path)
    else:
        raise ValueError('%s should exist!' % path)

    if isinstance(path, str):
        path = path.encode('utf-8')

    if ignore:
        patterns = []
        for exc in ignore:
            patterns.append(exc.encode('utf-8'))

        def dir_ok_fn_basic(dirpath, patterns=patterns):
            dname = os.path.basename(dirpath)
            for pattern_to_ignore in patterns:
                if pattern_to_ignore == dname:
                    return False
                if (pattern_to_ignore + b'/') in dirpath:
                    return False
            return True

        if ignore_empty_folder:
            def dir_ok_fn(dirpath, patterns=patterns):
                if not dir_ok_fn_basic(dirpath):
                    return False
                return os.listdir(dirpath) != []
        else:
            dir_ok_fn = dir_ok_fn_basic
    else:
        if ignore_empty_folder:
            def dir_ok_fn(dirpath):
                return os.listdir(dirpath) != []
        else:
            dir_ok_fn = git.default_validation_dir

    objects = git.compute_hashes_from_directory(
        path,
        dir_ok_fn=dir_ok_fn)

    h = objects[path]['checksums']

    return h


def init_svn_repo_from_archive_dump(archive_path, root_temp_dir='/tmp'):
    """Given a path to an archive containing an svn dump.
    Initialize an svn repository with the content of said dump.

    Returns:
        A tuple:
        - temporary folder: containing the mounted repository
        - repo_path, path to the mounted repository inside the temporary folder

    Raises:
        ValueError in case of failure to run the command to uncompress
        and load the dump.

    """
    project_name = os.path.basename(os.path.dirname(archive_path))
    temp_dir = tempfile.mkdtemp(suffix='.swh.loader.svn',
                                prefix='tmp.',
                                dir=root_temp_dir)

    try:
        repo_path = os.path.join(temp_dir, project_name)

        # create the repository that will be loaded with the dump
        cmd = ['svnadmin', 'create', repo_path]
        r = call(cmd)
        if r != 0:
            raise ValueError(
                'Failed to initialize empty svn repo for %s' %
                project_name)

        with Popen(['gzip', '-dc', archive_path], stdout=PIPE) as dump:
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
