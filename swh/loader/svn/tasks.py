# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.core import tasks

from .loader import GitSvnSvnLoader, SWHSvnLoader, SvnLoaderException
from .loader import converters


class LoadSvnRepositoryTsk(tasks.LoaderCoreTask):
    """Import one svn repository to Software Heritage.

    """
    CONFIG_BASE_FILENAME = 'loader/svn.ini'

    ADDITIONAL_CONFIG = {
        'storage_class': ('str', 'remote_storage'),
        'storage_args': ('list[str]', ['http://localhost:5000/']),
        'with_policy': ('string', 'swh'),  # Default, other possible
                                           # value is 'gitsvn'
    }

    task_queue = 'swh_loader_svn'

    def run(self, *args, **kwargs):
        """Import a svn repository.

        Args:
            - svn_url: svn's repository url
            - destination_path: root directory to locally retrieve svn's data
            - swh_revision: Optional extra swh revision to start from.
            cf. swh.loader.svn.SvnLoader.process docstring

        """
        destination_path = kwargs['destination_path']
        svn_url = kwargs['svn_url']

        if 'origin' not in kwargs:  # first time, we'll create the origin
            origin = {
                'type': 'svn',
                'url': svn_url,
            }
            origin['id'] = self.storage.origin_add_one(origin)
            retry = False
        else:
            origin = {
                'id': kwargs['origin'],
                'url': kwargs['svn_url'],
                'type': 'svn'
            }
            retry = True

        fetch_history_id = self.open_fetch_history(origin['id'])

        try:
            # Determine which loader to trigger
            if self.config['with_policy'] == 'gitsvn':
                # this one compute hashes but do not store anywhere
                loader = GitSvnSvnLoader(svn_url, destination_path, origin)
            elif self.config['with_policy'] == 'swh':
                # the real production use case with storage and all
                loader = SWHSvnLoader(svn_url, destination_path, origin)
            else:
                raise ValueError('Only gitsvn or swh policies are supported in'
                                 '\'with_policy\' entry. '
                                 'Please adapt your svn.ini file accordingly')
            if retry:
                swh_revision = converters.scheduler_to_loader_revision(
                    kwargs['swh_revision'])
                result = loader.load(swh_revision)
            else:
                result = loader.load()
        except SvnLoaderException as e:
            # reschedule a task
            swh_rev = converters.loader_to_scheduler_revision(e.swh_revision)
            self.scheduler_backend.create_task({
                'type': 'svn-loader',
                'arguments': {
                    'args': None,
                    'kwargs': {
                        'origin': origin['id'],
                        'svn_url': svn_url,
                        'destination_path': destination_path,
                        'swh_revision': swh_rev,
                    }
                }
            })
            self.log.error(
                'Error during loading: %s - Svn repository rescheduled.' % e)
            result = {'status': False}

        self.close_fetch_history(fetch_history_id, result)
