# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import sys


def get_task(task_name):
    """Retrieve task object in the application by its fully qualified name.

    """
    from swh.scheduler.celery_backend.config import app
    for module in app.conf.CELERY_IMPORTS:
        __import__(module)

    return app.tasks[task_name]


def _produce_svn_to_load(
        svn_url, original_svn_url, original_svn_uuid,
        destination_path=None, synchroneous=False,
        task_name='swh.loader.svn.tasks.LoadSWHSvnRepositoryTsk'):
    """Produce svn urls on the message queue.

    Those urls can either be read from stdin or directly passed as argument.

    """
    task = get_task(task_name)
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
            if svn_url:
                print(svn_url)
                task.delay(svn_url=svn_url,
                           original_svn_url=original_svn_url,
                           original_svn_uuid=original_svn_uuid,
                           destination_path=destination_path)


def _produce_archive_to_mount_and_load(
        archive_path,
        task_name='swh.loader.svn.tasks.MountAndLoadSvnRepositoryTsk'):
    task = get_task(task_name)
    if archive_path:
        task.delay(archive_path)
    else:
        for archive_path in sys.stdin:
            archive_path = archive_path.rstrip()
            if archive_path:
                print(archive_path)
                task.delay(archive_path)


@click.group()
def cli():
    pass


@cli.command('svn', help='Default svn urls producer')
@click.option('--url',
              help="svn repository's mirror url.")
@click.option('--original-url', default=None,
              help='svn repository\'s original remote url '
                   '(if different than --svn-url).')
@click.option('--original-uuid', default=None,
              help='svn repository\'s original uuid '
                   '(to provide when using --original-url)')
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
@click.option('--synchroneous',
              is_flag=True,
              help="To execute directly the svn loading.")
def produce_svn_to_load(url, original_url, original_uuid, destination_path,
                        synchroneous):
    _produce_svn_to_load(url, original_url, original_uuid,
                         destination_path=destination_path,
                         synchroneous=synchroneous)


@cli.command('svn-archive', help='Default svndump archive producer')
@click.option('--path', help="Archive's Path to load and mount")
def produce_archive_to_mount_and_load(path):
    _produce_archive_to_mount_and_load(path)


if __name__ == '__main__':
    cli()
