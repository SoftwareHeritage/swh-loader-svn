#!/usr/bin/env python3

# Use sample:
# hashtree.py --path . --ignore '.svn' --ignore '.git-svn'
# path: b'.'
# hash: 38f8d2c3a951f6b94007896d0981077e48bbd702

import click

from swh.model import git
from swh.core import hashutil


@click.command()
@click.option('--path', default='.',
              help='Optional path to hash.')
@click.option('--ignore', multiple=True,
              help='Ignore pattern.')
def hashtree(path, ignore=None):
    if isinstance(path, str):
        path = path.encode('utf-8')

    if ignore:
        patterns = []
        for exc in ignore:
            patterns.append(exc.encode('utf-8'))

        def dir_ok_fn(dirpath, patterns=patterns):
            for pattern_to_ignore in patterns:
                if pattern_to_ignore in dirpath:
                    return False

            return True

        objects = git.walk_and_compute_sha1_from_directory(
            path,
            dir_ok_fn=dir_ok_fn)
    else:
        objects = git.walk_and_compute_sha1_from_directory(path)

    h = objects[git.ROOT_TREE_KEY][0]['sha1_git']

    print('path: %s\nhash: %s' % (path, hashutil.hash_to_hex(h)))


if __name__ == '__main__':
    hashtree()
