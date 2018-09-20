# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import datetime
import sys

from swh.core import utils
from swh.scheduler.utils import get_task
from swh.scheduler.backend import SchedulerBackend


def _produce_svn_to_load(svn_url, origin_url,
                         destination_path=None, visit_date=None):
    """Yield svn url(s) parameters for producers.

    Those urls can either be read from stdin or directly passed as
    argument.  Either the svn_url is passed and only 1 svn url is
    sent.  Either no svn_url is provided and those urls are read from
    stdin and yielded as parameters for producers down the line.

    Args:
        svn_url (str / None): Potential svn url to load
        origin_url (str / None): Potential associated origin url
        destination_path (str): Destination path
        visit_date (date): Forcing the visit date

    Yields
        tuple svn_url, origin_url, visit_date, destination_path

    """
    if svn_url:
        yield svn_url, origin_url, visit_date, destination_path

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
            yield svn_url, origin_url, visit_date, destination_path


def _produce_archive_to_mount_and_load(archive_path, visit_date):
    """Yield svn dump(s) parameters for producers.

    Those dumps can either be read from stdin or directly passed as
    argument.  Either the archive_url is passed and only 1 dump is
    sent.  Either no archive_path is provided and those dumps are read
    from stdin and yielded as parameters for producers down the line.

    Args:
        svn_url (str / None): Potential svn url to load
        origin_url (str / None): Potential associated origin url
        destination_path (str): Destination path
        visit_date (date): Forcing the visit date

    Yields
        tuple archive_path, origin_url, visit_date

    """
    if archive_path:
        yield archive_path, None, visit_date

    for line in sys.stdin:
        line = line.rstrip()
        data = line.split(' ')
        archive_path = data[0]
        if len(data) > 1:
            origin_url = data[1]
        else:
            origin_url = None

        if archive_path:
            yield archive_path, origin_url, visit_date


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
@click.option('--dry-run/--no-dry-run', default=False, is_flag=True,
              help="Dry run flag")
@click.option('--start-from-scratch', default=False, is_flag=True,
              help="Start from scratch option")
def produce_svn_to_load(url, origin_url, destination_path, visit_date,
                        synchroneous, dry_run, start_from_scratch):
    """Produce svn urls to celery queue

    """
    task = get_task('swh.loader.svn.tasks.LoadSvnRepository')

    if synchroneous:
        fn = task
    else:
        fn = task.delay

    for args in _produce_svn_to_load(svn_url=url,
                                     origin_url=origin_url,
                                     visit_date=visit_date,
                                     destination_path=destination_path):
        print(args)
        if dry_run:
            continue

        svn_url, origin_url, visit_date, destination_path = args
        fn(svn_url=svn_url,
           origin_url=origin_url,
           visit_date=visit_date,
           destination_path=destination_path,
           start_from_scratch=start_from_scratch)


@cli.command('svn-archive', help='Default svndump archive producer')
@click.option('--visit-date',
              help="(optional) visit date to override")
@click.option('--path', help="Archive's Path to load and mount")
@click.option('--synchroneous',
              is_flag=True,
              help="To execute directly the svn loading.")
@click.option('--dry-run/--no-dry-run', default=False, is_flag=True,
              help="Dry run flag")
@click.option('--start-from-scratch', default=False, is_flag=True,
              help="Start from scratch option")
def produce_archive_to_mount_and_load(path, visit_date, synchroneous, dry_run,
                                      start_from_scratch):
    """Produce svn dumps to celery queue

    """
    task = get_task('swh.loader.svn.tasks.MountAndLoadSvnRepository')

    if synchroneous:
        fn = task
    else:
        fn = task.delay

    for args in _produce_archive_to_mount_and_load(path, visit_date):
        print(args)
        if dry_run:
            continue

        archive_path, origin_url, visit_date = args

        fn(archive_path, origin_url, visit_date,
           start_from_scratch=start_from_scratch)


@cli.command('schedule-svn-archive',
             help='Default svndump archive mounting and loading scheduling')
@click.option('--visit-date',
              help="(optional) visit date to override")
@click.option('--path', help="Archive's Path to load and mount")
@click.option('--dry-run/--no-dry-run', default=False, is_flag=True,
              help="Dry run flag")
def schedule_archive_to_mount_and_load(path, visit_date, dry_run):
    """Produce svn dumps to scheduler backend

    """
    scheduler = SchedulerBackend()

    def make_scheduler_task(path, origin_url, visit_date):
        return {
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
        }

    for tasks in utils.grouper(
            _produce_archive_to_mount_and_load(path, visit_date),
            n=1000):
        tasks = [make_scheduler_task(*t) for t in tasks]
        print('[%s, ...]' % tasks[0])
        if dry_run:
            continue

        scheduler.create_tasks(tasks)


if __name__ == '__main__':
    cli()
