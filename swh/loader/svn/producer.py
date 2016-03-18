import click

task_name = 'swh.loader.svn.tasks.LoadSvnRepositoryTsk'


@click.command()
@click.option('--svn-url',
              help="svn repository's remote url.")
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
def main(svn_url, destination_path):
    from swh.scheduler.celery_backend.config import app
    from swh.loader.svn import tasks  # noqa

    app.tasks[task_name].delay(svn_url, destination_path)


if __name__ == '__main__':
    main()
