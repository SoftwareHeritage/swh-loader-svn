# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .loader import SWHSvnLoader


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
