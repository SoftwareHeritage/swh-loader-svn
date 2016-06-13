#!/usr/bin/env python3

# Use sample:
# hashtree.py --path . --ignore '.svn' --ignore '.git-svn' \
#    --ignore-empty-folders
# 38f8d2c3a951f6b94007896d0981077e48bbd702

import click

from swh.core import hashutil

from swh.loader.svn.utils import hashtree


@click.command()
@click.option('--path', default='.',
              help='Optional path to hash.')
@click.option('--ignore-empty-folder', is_flag=True, default=False,
              help='Ignore empty folder.')
@click.option('--ignore', multiple=True,
              help='Ignore pattern.')
def main(path, ignore_empty_folder, ignore=None):
    h = hashtree(path, ignore_empty_folder, ignore)

    if h:
        print(hashutil.hash_to_hex(h['sha1_git']))


if __name__ == '__main__':
    main()
