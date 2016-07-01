# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import sys


task_name = 'swh.loader.svn.tasks.LoadSvnRepositoryTsk'


def libproduce(svn_url, original_svn_url, original_svn_uuid,
               destination_path=None, synchroneous=False):
    from swh.scheduler.celery_backend.config import app
    for module in app.conf.CELERY_IMPORTS:
        __import__(module)

    task = app.tasks[task_name]
    if not synchroneous and svn_url:
        task.delay(svn_url=svn_url,
                   original_svn_url=original_svn_url,
                   original_svn_uuid=original_svn_uuid,
                   destination_path=destination_path)
    elif synchroneous and svn_url:  # for debug purpose
        task(svn_url=svn_url,
             original_svn_url=original_svn_url,
             original_svn_uuid=original_svn_uuid,
             destination_path=destination_path)
    else:  # synchroneous flag is ignored in that case
        for svn_url in sys.stdin:
            svn_url = svn_url.rstrip()
            print(svn_url)
            task.delay(svn_url=svn_url,
                       original_svn_url=original_svn_url,
                       original_svn_uuid=original_svn_uuid,
                       destination_path=destination_path)


@click.command()
@click.option('--svn-url',
              help="svn repository's mirror url.")
@click.option('--original-svn-url', default=None,
              help='svn repository\'s original remote url '
                   '(if different than --svn-url).')
@click.option('--original-svn-uuid', default=None,
              help='svn repository\'s original uuid '
                   '(to provide when using --original-svn-url)')
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
@click.option('--synchroneous',
              is_flag=True,
              help="To execute directly the svn loading.")
def produce(svn_url, original_svn_url, original_svn_uuid, destination_path,
            synchroneous):
    libproduce(svn_url, original_svn_url, original_svn_uuid, destination_path,
               synchroneous)


if __name__ == '__main__':
    produce()
