# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pty
import unittest

from subprocess import Popen

from swh.loader.svn import utils


class TestUtils(unittest.TestCase):
    def test_strdate_to_timestamp(self):
        """Formatted string date should be converted in timestamp."""
        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:04:39.800722Z')
        self.assertEqual(actual_ts, {'seconds': 1306821879,
                                     'microseconds': 800722})

        actual_ts = utils.strdate_to_timestamp('2011-05-31T06:03:39.123450Z')
        self.assertEqual(actual_ts, {'seconds': 1306821819,
                                     'microseconds': 123450})

    def test_strdate_to_timestamp_empty_does_not_break(self):
        """Empty or None date should be timestamp 0."""
        self.assertEqual({'seconds': 0, 'microseconds': 0},
                         utils.strdate_to_timestamp(''))
        self.assertEqual({'seconds': 0, 'microseconds': 0},
                         utils.strdate_to_timestamp(None))

    def test_outputstream(self):
        stdout_r, stdout_w = pty.openpty()
        echo = Popen(['echo', '-e', 'foo\nbar\nbaz'], stdout=stdout_w)
        os.close(stdout_w)
        stdout_stream = utils.OutputStream(stdout_r)
        lines = []
        while True:
            current_lines, readable = stdout_stream.read_lines()
            lines += current_lines
            if not readable:
                break
        echo.wait()
        os.close(stdout_r)
        self.assertEqual(lines, ['foo', 'bar', 'baz'])
