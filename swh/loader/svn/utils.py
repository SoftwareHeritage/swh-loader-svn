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
