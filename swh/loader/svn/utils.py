# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from dateutil import parser

from swh.model import git


def strdate_to_timestamp(strdate):
    """Convert a string date to an int timestamp.

    """
    if not strdate:  # epoch
        return 0
    dt = parser.parse(strdate)
    ts_float = dt.timestamp()
    return int(ts_float)


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


def hashtree(path, ignore_empty_folder, ignore=None):
    """Given a path and options, compute the hash's upper tree.

    This is not for production use.
    It's merely a helper function used mainly in bin/hashtree.py

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
            print('%s should be a directory!' % path)
            return
    else:
        print('%s should exist!' % path)
        return

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
