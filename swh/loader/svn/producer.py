# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import sys


task_name = 'swh.loader.svn.tasks.LoadSvnRepositoryTsk'


@click.command()
@click.option('--svn-url',
              help="svn repository's remote url.")
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
def main(svn_url, destination_path):
    from swh.scheduler.celery_backend.config import app
    from swh.loader.svn import tasks  # noqa

    if svn_url:
        app.tasks[task_name].delay(svn_url, destination_path)
    else:
        for svn_url in sys.stdin:
            svn_url = svn_url.rstrip()
            print(svn_url)
            app.tasks[task_name].delay(svn_url, destination_path)


if __name__ == '__main__':
    main()
