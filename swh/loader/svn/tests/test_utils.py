# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import utils


class TestUtils(unittest.TestCase):
    @istest
    def strdate_to_timestamp(self):
        """Formatted string date should be converted in timestamp."""
        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:04:39.800722Z')
        self.assertEquals(actual_ts, {'seconds': 1306821879,
                                      'microseconds': 800722})

        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:03:39.123450Z')
        self.assertEquals(actual_ts, {'seconds': 1306821819,
                                      'microseconds': 123450})

    @istest
    def strdate_to_timestamp_empty_does_not_break(self):
        """Empty or None date should be timestamp 0."""
        self.assertEquals({'seconds': 0, 'microseconds': 0},
                          utils.strdate_to_timestamp(''))
        self.assertEquals({'seconds': 0, 'microseconds': 0},
                          utils.strdate_to_timestamp(None))
