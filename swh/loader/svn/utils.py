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
    - swh.loader.svn.ra compute relative paths
    - swh.model.git compute with full paths

    FIXME: Need to walk the paths and transform the path relative to
    the root (that's how swh.loader.svn.ra computes the path)

    """
    hashes[b''] = hashes[rootpath]
    hashes.pop(rootpath, None)
    return hashes
