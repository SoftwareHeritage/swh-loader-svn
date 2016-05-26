# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.svn import svn


class TestBuildUtils(unittest.TestCase):
    @istest
    def ignore_dot_svn_folder(self):
        self.assertTrue(svn.ignore_dot_svn_folder(b''))
        self.assertTrue(svn.ignore_dot_svn_folder(b'/some/path/with/svn'))
        self.assertTrue(svn.ignore_dot_svn_folder(
            b'/path/with/no/reference/to/svn'))
        self.assertTrue(svn.ignore_dot_svn_folder(b'/some/file.svnlike/files'))
        self.assertTrue(svn.ignore_dot_svn_folder(b'/some/thing.svn/files'))

        self.assertFalse(svn.ignore_dot_svn_folder(b'.svn'))
        self.assertFalse(svn.ignore_dot_svn_folder(b'/some/path/.svn/files'))
