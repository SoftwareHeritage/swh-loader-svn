# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .loader import SWHSvnLoader, SWHSvnLoaderFromDumpArchive


class LoadSWHSvnRepositoryTsk(Task):
    """Import one svn repository to Software Heritage.

    """
    task_queue = 'swh_loader_svn'

    def run_task(self, *args, **kwargs):
        """Import a svn repository with swh policy.

        Args:
            args: ordered arguments (expected None)
            kwargs: Dictionary with the following expected keys:

              - svn_url: (mandatory) svn's repository url
              - destination_path: (mandatory) root directory to
                locally retrieve svn's data
              - swh_revision: (optional) extra SWH revision hex to
                start from.  see swh.loader.svn.SvnLoader.process
                docstring

        """
        loader = SWHSvnLoader()
        loader.log = self.log
        loader.load(*args, **kwargs)


class MountAndLoadSvnRepositoryTsk(Task):
    task_queue = 'swh_loader_svn_mount_and_load'

    def run_task(self, archive_path, origin_url=None, visit_date=None):
        """1. Mount an svn dump from archive as a local svn repository.
           2. Load it through the svn loader.
           3. Clean up mounted svn repository archive.

        """

        loader = SWHSvnLoaderFromDumpArchive(archive_path)
        loader.log = self.log
        loader.load(svn_url='file://%s' % loader.repo_path,
                    origin_url=origin_url,
                    visit_date=visit_date,
                    destination_path=None)
