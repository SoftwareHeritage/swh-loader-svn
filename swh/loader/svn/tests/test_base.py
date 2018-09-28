# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile
import unittest

from swh.model import hashutil


class BaseSvnLoaderTest(unittest.TestCase):
    """Base test loader class.

    In its setup, it's uncompressing a local svn mirror to /tmp.

    """
    def setUp(self, archive_name='pkg-gourmet.tgz', filename='pkg-gourmet'):
        self.tmp_root_path = tempfile.mkdtemp()

        start_path = os.path.dirname(__file__)
        svn_mirror_repo = os.path.join(start_path,
                                       'resources',
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

    def assertSnapshotOk(self, expected_snapshot, expected_branches):
        snapshots = self.loader.all_snapshots
        self.assertEqual(len(snapshots), 1)

        snap = snapshots[0]
        snap_id = hashutil.hash_to_hex(snap['id'])
        self.assertEqual(snap_id, expected_snapshot)

        def decode_target(target):
            if not target:
                return target
            target_type = target['target_type']

            if target_type == 'alias':
                decoded_target = target['target'].decode('utf-8')
            else:
                decoded_target = hashutil.hash_to_hex(target['target'])

            return {
                'target': decoded_target,
                'target_type': target_type
            }

        branches = {
            branch.decode('utf-8'): decode_target(target)
            for branch, target in snap['branches'].items()
        }
        self.assertEqual(expected_branches, branches)

    def assertRevisionsOk(self, expected_revisions):  # noqa: N802
        """Check the loader's revisions match the expected revisions.

        Expects self.loader to be instantiated and ready to be
        inspected (meaning the loading took place).

        Args:
            expected_revisions (dict): Dict with key revision id,
            value the targeted directory id.

        """
        # The last revision being the one used later to start back from
        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)
