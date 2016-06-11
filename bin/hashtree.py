#!/usr/bin/env python3

# Use sample:
# hashtree.py --path . --ignore '.svn' --ignore '.git-svn' \
#    --ignore-empty-folders
# 38f8d2c3a951f6b94007896d0981077e48bbd702

import click
import os

from swh.model import git
from swh.core import hashutil


@click.command()
@click.option('--path', default='.',
              help='Optional path to hash.')
@click.option('--ignore-empty-folder', is_flag=True, default=False,
              help='Ignore empty folder.')
@click.option('--ignore', multiple=True,
              help='Ignore pattern.')
def hashtree(path, ignore_empty_folder, ignore=None):
    if os.path.exists(path):
        if not os.path.isdir(path):
            print('%s should be a directory!' % path)
            return
    else:
        print('%s should exist!' % path)
        return

    if isinstance(path, str):
        path = path.encode('utf-8')

    if ignore:
        patterns = []
        for exc in ignore:
            patterns.append(exc.encode('utf-8'))

        def dir_ok_fn_basic(dirpath, patterns=patterns):
            dname = os.path.basename(dirpath)
            for pattern_to_ignore in patterns:
                if pattern_to_ignore == dname:
                    return False
                if (pattern_to_ignore + b'/') in dirpath:
                    return False
            return True

        if ignore_empty_folder:
            def dir_ok_fn(dirpath, patterns=patterns):
                if not dir_ok_fn_basic(dirpath):
                    return False
                return os.listdir(dirpath) != []
        else:
            dir_ok_fn = dir_ok_fn_basic
    else:
        if ignore_empty_folder:
            def dir_ok_fn(dirpath):
                return os.listdir(dirpath) != []
        else:
            dir_ok_fn = git.default_validation_dir

    objects = git.compute_sha1_from_directory(
        path,
        dir_ok_fn=dir_ok_fn)

    h = objects[path]['checksums']['sha1_git']

    print(hashutil.hash_to_hex(h))


if __name__ == '__main__':
    hashtree()
