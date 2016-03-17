# 1. checkout svn repository

import click
import os
import svn.remote as remote
import svn.local as local
import tempfile

from contextlib import contextmanager

from swh.core import hashutil
from swh.loader.dir.git import walk_and_compute_sha1_from_directory
from swh.loader.dir.git import ROOT_TREE_KEY


def checkout_repo(remote_repo_url, destination_path=None):
    """Checkout a remote repository locally.

    Args:
        remote_repo_url: The remote svn url
        destination_path: The optional local parent folder to checkout the
        repository to

    Returns:
        Dictionary with the following keys:
            - remote: remote instance to manipulate the repo
            - local: local instance to manipulate the local repo
            - remote_url: remote url (same as input)
            - local_url: local url which has been computed

    """
    name = os.path.basename(remote_repo_url)
    if destination_path:
        os.makedirs(destination_path, exist_ok=True)
        local_dirname = destination_path
    else:
        local_dirname = tempfile.mkdtemp(suffix='swh.loader.svn.',
                                         prefix='tmp.',
                                         dir='/tmp')

    local_repo_url = os.path.join(local_dirname, name)

    remote_client = remote.RemoteClient(remote_repo_url)
    remote_client.checkout(local_repo_url)

    return {'remote': remote_client,
            'local': local.LocalClient(local_repo_url),
            'remote_url': remote_repo_url,
            'local_url': local_repo_url}


def retrieve_last_known_revision(remote_url_repo):
    """Function that given a remote url returns the last known revision or
    1 if this is the first time.

    """
    # TODO: Contact swh-storage to retrieve the last occurrence for
    # the given origin
    return 1


@contextmanager
def cwd(path):
    """A context manager which changes the working directory to the given
       path, and then changes it back to its previous value on exit.

    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def git_history(repo, latest_revision):
    rev = retrieve_last_known_revision(repo['remote_url'])
    if rev == latest_revision:
        return None

    with cwd(repo['local_url']):
        while rev != latest_revision:
            # checkout to the revision rev
            repo['remote'].checkout(revision=rev, path='.')

            # compute git commit
            parsed_objects = walk_and_compute_sha1_from_directory(
                repo['local_url'].encode('utf-8'))
            # print(parsed_objects)

            root_tree = parsed_objects[ROOT_TREE_KEY][0]
            print('rev: %s -> tree %s' % (rev,
                                          hashutil.hash_to_hex(
                                              root_tree['sha1_git'])))

            yield parsed_objects

            rev += 1


@click.command()
@click.option('--svn-url',
              help="svn repository's remote url.")
@click.option('--destination-path',
              help="(optional) svn checkout destination.")
def main(svn_url, destination_path):
    repo = checkout_repo(svn_url, destination_path)

    # 2. retrieve current svn revision

    repo_metadata = repo['local'].info()

    latest_revision = repo_metadata['entry_revision']
    print(latest_revision)

    for data in git_history(repo, latest_revision):
        print('data: %s' % data)
    # 3. go to first svn revision for trunk since last time (the first
    # time, it's the revision 1)

    # 5. compute filesystem tree representation for trunk

    # 6. compute a commit (swh revision) `a la git`
    # (cf. swh-loader-dir/swh-model) pointing on that tree

    # 7. reference the original revision in the swh revision metadata

    # 8. go to next svn revision (don't forget to update revision history)
    # (revision 1 has no parents, revision 2 has parents revision 1,
    # etc...)

    # 9. when arrived at original svn revision (step 2)

    # 10. list branches/tags

    # 11. create occurrences using mapping svn revision <-> swh revision

    # a good way to ascertain everything is ok could be to use git-svn to
    # check the sha1 are ok


if __name__ == '__main__':
    main()
