# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.config import load_named_config
from swh.scheduler.task import Task

from swh.loader.svn.loader import SvnLoaderWithHistory


DEFAULT_CONFIG = {
    'storage_class': ('str', 'remote_storage'),
    'storage_args': ('list[str]', ['http://localhost:5000/']),
    'send_contents': ('bool', True),
    'send_directories': ('bool', True),
    'send_revisions': ('bool', True),
    'send_releases': ('bool', True),
    'send_occurrences': ('bool', True),
    'content_packet_size': ('int', 10000),
    'content_packet_size_bytes': ('int', 1073741824),
    'directory_packet_size': ('int', 25000),
    'revision_packet_size': ('int', 100000),
    'release_packet_size': ('int', 100000),
    'occurrence_packet_size': ('int', 100000),
}


class LoadSvnRepositoryTsk(Task):
    """Import a svn repository to Software Heritage

    """
    task_queue = 'swh_loader_svn'

    @property
    def config(self):
        if not hasattr(self, '__config'):
            self.__config = load_named_config(
                'loader/svn.ini',
                DEFAULT_CONFIG)
        return self.__config

    def run(self, svn_url, local_path):
        """Import a svn repository.

        Args:
            cf. swh.loader.svn.loader.process docstring

        """
        loader = SvnLoaderWithHistory(self.config)
        loader.log = self.log
        loader.process(svn_url, local_path)
