# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.config import load_named_config
from swh.loader.vcs import tasks
from swh.loader.svn.loader import SvnLoader
from swh.storage import get_storage


DEFAULT_CONFIG = {
    'storage_class': ('str', 'remote_storage'),
    'storage_args': ('list[str]', ['http://localhost:5000/']),
    'send_contents': ('bool', True),
    'send_directories': ('bool', True),
    'send_revisions': ('bool', True),
    'send_releases': ('bool', True),
    'send_occurrences': ('bool', True),
    'content_packet_size': ('int', 10000),
    'content_packet_block_size_bytes': ('int', 100 * 1024 * 1024),
    'content_packet_size_bytes': ('int', 1024 * 1024 * 1024),
    'directory_packet_size': ('int', 25000),
    'revision_packet_size': ('int', 100),
    'release_packet_size': ('int', 100000),
    'occurrence_packet_size': ('int', 100000),
}


class LoadSvnRepositoryTsk(tasks.LoaderCoreTask):
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
            cf. swh.loader.svn.SvnLoader.process docstring

        """
        config = self.config
        storage = get_storage(config['storage_class'], config['storage_args'])

        origin = {'type': 'svn', 'url': svn_url}
        origin['id'] = storage.origin_add_one(origin)

        fetch_history_id = self.open_fetch_history(storage, origin['id'])

        # try:
        result = SvnLoader(config, origin['id']).process(svn_url,
                                                         origin,
                                                         local_path)
        # except:
        #     e_info = sys.exc_info()
        #     self.log.error('Problem during svn load for repo %s - %s' % (
        #         svn_url, e_info[1]))
        #     result = {'status': False, 'stderr': 'reason:%s\ntrace:%s' % (
        #             e_info[1],
        #             ''.join(traceback.format_tb(e_info[2])))}

        self.close_fetch_history(storage, fetch_history_id, result)