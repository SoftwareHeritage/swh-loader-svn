# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import datetime
import sys

from swh.scheduler.utils import get_task
from swh.scheduler.backend import SchedulerBackend


def _produce_svn_to_load(
        svn_url, origin_url,
        destination_path=None, visit_date=None, synchroneous=False,
        callable_fn=lambda x: x):
    """Produce svn urls on the message queue.

    Those urls can either be read from stdin or directly passed as argument.

    """
    if svn_url:
        return callable_fn(svn_url=svn_url,
                           origin_url=origin_url,
                           visit_date=visit_date,
                           destination_path=destination_path)

    # input from stdin, so we ignore most of the function's input
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
            callable_fn(svn_url=svn_url,
                        origin_url=origin_url,
                        visit_date=visit_date,
                        destination_path=destination_path)


def _produce_archive_to_mount_and_load(archive_path, visit_date, callable_fn):
    if archive_path:
        return callable_fn(archive_path, origin_url=None)

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
            callable_fn(archive_path, origin_url, visit_date)


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
    task = get_task('swh.loader.svn.tasks.LoadSWHSvnRepositoryTsk')

    def callable_fn(svn_url, origin_url, destination_path, visit_date,
                    synchroneous=synchroneous, task=task):
        if synchroneous:
            fn = task
        else:
            fn = task.delay

        fn(svn_url=svn_url,
           origin_url=origin_url,
           visit_date=visit_date,
           destination_path=destination_path)

    _produce_svn_to_load(svn_url=url,
                         origin_url=origin_url,
                         visit_date=visit_date,
                         destination_path=destination_path,
                         callable_fn=callable_fn)


@cli.command('svn-archive', help='Default svndump archive producer')
@click.option('--visit-date',
              help="(optional) visit date to override")
@click.option('--path', help="Archive's Path to load and mount")
@click.option('--synchroneous',
              is_flag=True,
              help="To execute directly the svn loading.")
def produce_archive_to_mount_and_load(path, visit_date, synchroneous):
    task = get_task('swh.loader.svn.tasks.MountAndLoadSvnRepositoryTsk')

    def callable_fn(path, origin_url, visit_date=visit_date,
                    synchroneous=synchroneous, task=task):
        if synchroneous:
            fn = task
        else:
            fn = task.delay

        fn(path, origin_url, visit_date)

    _produce_archive_to_mount_and_load(path, visit_date, callable_fn)


@cli.command('schedule-svn-archive',
             help='Default svndump archive mounting and loading scheduling')
@click.option('--visit-date',
              help="(optional) visit date to override")
@click.option('--path', help="Archive's Path to load and mount")
@click.option('--dry-run/--no-dry-run', default=False, is_flag=True,
              help="Dry run flag")
def schedule_archive_to_mount_and_load(path, visit_date, dry_run):
    scheduler = SchedulerBackend()

    def callable_fn(path, origin_url, visit_date, scheduler=scheduler,
                    dry_run=dry_run):
        tasks = [{
            'policy': 'oneshot',
            'type': 'swh-loader-mount-dump-and-load-svn-repository',
            'next_run': datetime.datetime.now(tz=datetime.timezone.utc),
            'arguments': {
                'args': [
                    path,
                ],
                'kwargs': {
                    'origin_url': origin_url,
                    'visit_date': visit_date,
                },
            }
        }]

        print(tasks)
        if not dry_run:
            scheduler.create_tasks(tasks)

    _produce_archive_to_mount_and_load(path, visit_date, callable_fn)


if __name__ == '__main__':
    cli()
