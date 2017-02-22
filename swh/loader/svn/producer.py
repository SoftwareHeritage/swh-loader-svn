# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import sys

from swh.scheduler.utils import get_task


def _produce_svn_to_load(
        svn_url, origin_url,
        destination_path=None, visit_date=None, synchroneous=False,
        task_name='swh.loader.svn.tasks.LoadSWHSvnRepositoryTsk'):
    """Produce svn urls on the message queue.

    Those urls can either be read from stdin or directly passed as argument.

    """
    task = get_task(task_name)
    if not synchroneous and svn_url:
        task.delay(svn_url=svn_url,
                   origin_url=origin_url,
                   visit_date=visit_date,
                   destination_path=destination_path)
    elif synchroneous and svn_url:  # for debug purpose
        task(svn_url=svn_url,
             origin_url=origin_url,
             visit_date=visit_date,
             destination_path=destination_path)
    else:  # input from stdin, so we ignore most of the function's input
        for line in sys.stdin:
            line = line.rstrip()
            data = line.split(' ')
            svn_url = data[0]
            if len(data) > 1:
                origin_url = data[1]
            else:
                origin_url = None

            if svn_url:
                print(svn_url, origin_url)
                task.delay(svn_url=svn_url,
                           origin_url=origin_url,
                           visit_date=visit_date,
                           destination_path=destination_path)


def _produce_archive_to_mount_and_load(
        archive_path,
        visit_date,
        task_name='swh.loader.svn.tasks.MountAndLoadSvnRepositoryTsk'):
    task = get_task(task_name)
    if archive_path:
        task.delay(archive_path)
    else:
        for line in sys.stdin:
            line = line.rstrip()
            data = line.split(' ')
            archive_path = data[0]
            if len(data) > 1:
                origin_url = data[1]
            else:
                origin_url = None

            if archive_path:
                print(archive_path, origin_url)
                task.delay(archive_path, origin_url, visit_date=visit_date)


@click.group()
def cli():
    pass


@cli.command('svn', help='Default svn urls producer')
@click.option('--url',
              help="svn repository's mirror url.")
@click.option('--origin-url', default=None,
              help='svn repository\'s original remote url '
                   '(if different than --svn-url).')
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
@click.option('--visit-date',
              help="(optional) visit date to override")
@click.option('--synchroneous',
              is_flag=True,
              help="To execute directly the svn loading.")
def produce_svn_to_load(url, origin_url,
                        destination_path, visit_date, synchroneous):
    _produce_svn_to_load(svn_url=url,
                         origin_url=origin_url,
                         visit_date=visit_date,
                         destination_path=destination_path,
                         synchroneous=synchroneous)


@cli.command('svn-archive', help='Default svndump archive producer')
@click.option('--visit-date',
              help="(optional) visit date to override")
@click.option('--path', help="Archive's Path to load and mount")
def produce_archive_to_mount_and_load(path, visit_date):
    _produce_archive_to_mount_and_load(path, visit_date)


if __name__ == '__main__':
    cli()
