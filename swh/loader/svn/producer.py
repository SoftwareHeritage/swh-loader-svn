# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import sys


task_name = 'swh.loader.svn.tasks.LoadSvnRepositoryTsk'


def libproduce(svn_url, destination_path=None, synchroneous=False):
    from swh.scheduler.celery_backend.config import app
    from swh.loader.svn import tasks  # noqa

    task = app.tasks[task_name]
    if not synchroneous and svn_url:
        task.delay(svn_url, destination_path)
    elif synchroneous and svn_url:  # for debug purpose
        task(svn_url, destination_path)
    else:  # synchroneous flag is ignored in that case
        for svn_url in sys.stdin:
            svn_url = svn_url.rstrip()
            print(svn_url)
            task.delay(svn_url, destination_path)


@click.command()
@click.option('--svn-url',
              help="svn repository's remote url.")
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
@click.option('--synchroneous',
              is_flag=True,
              help="To execute directly the svn loading.")
def produce(svn_url, destination_path, synchroneous):
    libproduce(svn_url, destination_path, synchroneous)


if __name__ == '__main__':
    produce()
