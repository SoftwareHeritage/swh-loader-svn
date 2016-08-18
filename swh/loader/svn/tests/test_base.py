# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile
import unittest


PATH_TO_DATA = '../../../../..'


class BaseTestSvnLoader(unittest.TestCase):
    """Base test loader class.

    In its setup, it's uncompressing a local svn mirror to /tmp.

    """
    def setUp(self, archive_name='pkg-gourmet.tgz', filename='pkg-gourmet'):
        self.tmp_root_path = tempfile.mkdtemp()

        start_path = os.path.dirname(__file__)
        svn_mirror_repo = os.path.join(start_path,
                                       PATH_TO_DATA,
                                       'swh-storage-testdata',
                                       'svn-folders',
                                       archive_name)

        # uncompress the sample folder
        subprocess.check_output(
            ['tar', 'xvf', svn_mirror_repo, '-C', self.tmp_root_path],
        )

        self.svn_mirror_url = 'file://' + self.tmp_root_path + '/' + filename
        self.destination_path = os.path.join(
            self.tmp_root_path, 'working-copy')

    def tearDown(self):
        shutil.rmtree(self.tmp_root_path)


class BaseTestTreeLoader(unittest.TestCase):
    """Root class to ease walk and git hash testing without side-effecty
    problems.

    """
    def setUp(self):
        super().setUp()
        self.tmp_root_path = tempfile.mkdtemp()
        self.maxDiff = None

        start_path = os.path.dirname(__file__)
        sample_folder = os.path.join(start_path,
                                     PATH_TO_DATA,
                                     'swh-storage-testdata',
                                     'dir-folders',
                                     'sample-folder.tgz')

        self.root_path = os.path.join(self.tmp_root_path, 'sample-folder')

        # uncompress the sample folder
        subprocess.check_output(
            ['tar', 'xvf', sample_folder, '-C', self.tmp_root_path])

    def tearDown(self):
        if os.path.exists(self.tmp_root_path):
            shutil.rmtree(self.tmp_root_path)
