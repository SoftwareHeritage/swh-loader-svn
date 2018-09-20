# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .loader import SvnLoader, SvnLoaderFromDumpArchive


class LoadSvnRepository(Task):
    """Import one svn repository to Software Heritage.

    """
    task_queue = 'swh_loader_svn'

    def run_task(self, *, svn_url,
                 destination_path=None,
                 swh_revision=None,
                 origin_url=None,
                 visit_date=None,
                 start_from_scratch=None):
        """Import a svn repository with swh policy.

        Args:
            args: ordered arguments (expected None)
            kwargs: Dictionary with the following expected keys:

              - svn_url (str): (mandatory) svn's repository url
              - destination_path (str): (mandatory) root directory to
                locally retrieve svn's data
              - origin_url (str): Optional original url override
              - swh_revision (dict): (optional) extra SWH revision hex to
                start from.  see swh.loader.svn.SvnLoader.process
                docstring

        """
        loader = SvnLoader()
        loader.log = self.log
        return loader.load(
            svn_url=svn_url,
            destination_path=destination_path,
            origin_url=origin_url,
            swh_revision=swh_revision,
            visit_date=visit_date,
            start_from_scratch=start_from_scratch)


class MountAndLoadSvnRepository(Task):
    task_queue = 'swh_loader_svn_mount_and_load'

    def run_task(self, *, archive_path, origin_url=None, visit_date=None,
                 start_from_scratch=False):
        """1. Mount an svn dump from archive as a local svn repository.
           2. Load it through the svn loader.
           3. Clean up mounted svn repository archive.

        """
        loader = SvnLoaderFromDumpArchive(archive_path)
        loader.log = self.log
        return loader.load(svn_url='file://%s' % loader.repo_path,
                           origin_url=origin_url,
                           visit_date=visit_date,
                           archive_path=archive_path,
                           start_from_scratch=start_from_scratch)
