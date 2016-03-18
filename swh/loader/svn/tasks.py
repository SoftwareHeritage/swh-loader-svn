# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from swh.loader.svn.loader import SvnLoaderWithHistory


class LoadSvnRepositoryTsk(Task):
    """Import a directory to Software Heritage

    """
    task_queue = 'swh_loader_svn'

    CONFIG_BASE_FILENAME = 'loader/svn.ini'
    ADDITIONAL_CONFIG = {}

    def __init__(self):
        self.config = SvnLoaderWithHistory.parse_config_file(
            base_filename=self.CONFIG_BASE_FILENAME,
            additional_configs=[self.ADDITIONAL_CONFIG],
        )

    def run(self, svn_url, local_path):
        """Import a svn repository.

        Args:
            cf. swh.loader.svn.loader.process docstring

        """
        loader = SvnLoaderWithHistory(self.config)
        loader.log = self.log
        loader.process(svn_url, local_path)
