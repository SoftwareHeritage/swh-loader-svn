# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil

from os.path import basename

from swh.scheduler.task import Task

from .loader import SWHSvnLoader
from . import utils


class LoadSWHSvnRepositoryTsk(Task):
    """Import one svn repository to Software Heritage.

    """
    task_queue = 'swh_loader_svn'

    def run(self, *args, **kwargs):
        """Import a svn repository with swh policy.

        Args:
            args: ordered arguments (expected None)
            kwargs: Dictionary with the following expected keys:
              - svn_url: (mandatory) svn's repository url
              - destination_path: (mandatory) root directory to
                locally retrieve svn's data
              - swh_revision: (optional) extra SWH revision hex to
                start from.  cf. swh.loader.svn.SvnLoader.process
                docstring

        """
        SWHSvnLoader().load(*args, **kwargs)


class MountAndLoadSvnRepositoryTsk(Task):
    task_queue = 'swh_mount_and_load_loader_svn'

    def run(self, archive_path):
        """1. Mount an svn dump from archive as a local svn repository.
           2. Load it through the svn loader.
           3. Clean up mounted svn repository archive.
        """
        self.log.info('Archive to mount and load %s' % archive_path)
        temp_dir, repo_path = utils.init_svn_repo_from_archive_dump(
            archive_path)
        self.log.debug('Mounted svn repository to %s' % repo_path)
        try:
            SWHSvnLoader().load(svn_url='file://%s' % repo_path,
                                destination_path=None)
        except Exception as e:
            raise e
        finally:
            self.log.debug('Clean up temp directory %s for project %s' % (
                temp_dir, basename(repo_path)))
            shutil.rmtree(temp_dir)
