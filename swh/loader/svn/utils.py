# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dateutil import parser


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
