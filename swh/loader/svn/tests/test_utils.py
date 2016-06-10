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
        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:04:39.800722Z')

        self.assertEquals(actual_ts, 1306821879)

    @istest
    def strdate_to_timestamp_empty_does_not_break(self):
        # It should return 0, epoch
        self.assertEquals(0, utils.strdate_to_timestamp(''))
