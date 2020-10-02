# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pty
from subprocess import Popen

from swh.loader.svn import utils
from swh.model.model import Timestamp


def test_outputstream():
    stdout_r, stdout_w = pty.openpty()
    echo = Popen(["echo", "-e", "foo\nbar\nbaz"], stdout=stdout_w)
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
    assert lines == ["foo", "bar", "baz"]


def test_strdate_to_timestamp():
    """Formatted string date should be converted in timestamp."""
    actual_ts = utils.strdate_to_timestamp("2011-05-31T06:04:39.800722Z")
    assert actual_ts == Timestamp(seconds=1306821879, microseconds=800722)

    actual_ts = utils.strdate_to_timestamp("2011-05-31T06:03:39.123450Z")
    assert actual_ts == Timestamp(seconds=1306821819, microseconds=123450)


def test_strdate_to_timestamp_empty_does_not_break():
    """Empty or None date should be timestamp 0."""
    default_ts = Timestamp(seconds=0, microseconds=0)
    assert default_ts == utils.strdate_to_timestamp("")
    assert default_ts == utils.strdate_to_timestamp(None)
