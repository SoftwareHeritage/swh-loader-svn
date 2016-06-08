# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import os
import shutil
import tempfile

from subvertpy import delta, properties
from subvertpy.ra import RemoteAccess, Auth, get_username_provider

from swh.core.hashutil import hex_to_hash
from swh.model import git, hashutil


def compute_svn_link_metadata(linkpath, filetype, data):
    """Given a svn linkpath (raw file with format 'link <src-link>'),
    compute the git metadata.

    Args:
        linkpath: absolute pathname of the svn link
        filetype: the file's type according to svn, should only be 'link'
        data: the link's content a.k.a the link's source file

    Returns:
        Dictionary of values:
            - data: link's content
            - length: link's content length
            - name: basename of the link
            - perms: git permission for link
            - type: git type for link
            - path: absolute path to the link on filesystem

    Raises:
        ValueError if the filetype does not match 'link'.

    """
    if filetype != b'link':
        raise ValueError(
            'Do not deal with other type (%s) than link.' % (
                linkpath, type))

    link_metadata = hashutil.hash_data(data)
    link_metadata.update({
        'data': data,
        'length': len(data),
        'name': os.path.basename(linkpath),
        'perms': git.GitPerm.LINK,
        'type': git.GitType.BLOB,
        'path': linkpath
    })

    return link_metadata


class SWHFileEditor:
    """File Editor in charge of updating file on disk and memory hashes.

    """
    __slots__ = ['state', 'path', 'fullpath', 'executable', 'link']

    def __init__(self, state, rootpath, path):
        self.state = state
        self.path = path
        # default value: 0, 1: set the flag, 2: remove the exec flag
        self.executable = 0
        self.link = None
        self.fullpath = os.path.join(rootpath, path)

    def change_prop(self, key, value):
        if key == properties.PROP_EXECUTABLE:
            if value is None:  # bit flip off
                self.executable = 2
            else:
                self.executable = 1
        elif key == properties.PROP_SPECIAL:
            self.link = True

    def __make_symlink(self):
        """Convert the svnlink to a symlink on disk.

        This function expects self.fullpath to be a svn link.

        Return:
            The svnlink's data tuple:
            - type (should be only 'link')
            - <path-to-src>

        """
        with open(self.fullpath, 'rb') as f:
            filetype, src = f.read().split(b' ')

        os.remove(self.fullpath)
        os.symlink(src=src, dst=self.fullpath)
        return filetype, src

    def __make_svnlink(self):
        """Convert the symlink to a svnlink on disk.

        Return:
            The symlink's svnlink data (b'type <path-to-src>')

        """
        # we replace the symlink by a svnlink
        src = os.readlink(self.fullpath)
        os.remove(self.fullpath)
        # to be able to patch the file
        sbuf = b'link ' + src
        with open(self.fullpath, 'wb') as f:
            f.write(sbuf)
        return sbuf

    def apply_textdelta(self, base_checksum):
        if os.path.lexists(self.fullpath):
            if os.path.islink(self.fullpath):
                sbuf = self.__make_svnlink()
                self.link = True
            else:
                sbuf = open(self.fullpath, 'rb').read()
        else:
            sbuf = b''
        f = open(self.fullpath, 'wb')
        return delta.apply_txdelta_handler(sbuf, target_stream=f)

    def close(self):
        """When done with the file, this is called.
        So the file exists and is updated, we can:
        - adapt accordingly its execution flag if any
        - compute the hashes

        """
        if self.link:
            filetype, source_link = self.__make_symlink()
            self.state[self.path] = {
                'checksums': compute_svn_link_metadata(self.fullpath,
                                                       filetype=filetype,
                                                       data=source_link)
            }
            return

        if self.executable == 1:
            os.chmod(self.fullpath, 0o755)
        elif self.executable == 2:
            os.chmod(self.fullpath, 0o644)

        # And now compute file's checksums
        self.state[self.path] = {
            'checksums': git.compute_blob_metadata(self.fullpath)
        }


def default_dictionary():
    """Default dictionary.

    """
    return dict(checksums=None, children=set())


class SWHDirEditor:
    """Directory Editor in charge of updating directory hashes computation.

    """
    __slots__ = ['state', 'rootpath', 'path']

    def __init__(self, state, rootpath, path):
        self.state = state
        self.rootpath = rootpath
        self.path = path
        # build directory on init
        os.makedirs(os.path.join(rootpath, path), exist_ok=True)

    def __add_child(self, path):
        """Add a children path to the actual state for the current directory
        seen as the parent.

        Args:
            path: The child to add

        """
        d = self.state.get(self.path, default_dictionary())
        d['children'].add(path)
        self.state[self.path] = d

    def __remove_child(self, path):
        """Remove a path from the current state.

        The path can be resolved as link, file or directory.

        This function takes also care of removing the link between the
        child and the parent.

        Args:
            path: to remove from the current state.

        """
        entry_removed = self.state.pop(path, None)
        fpath = os.path.join(self.rootpath, path)
        if entry_removed:
            if 'children' in entry_removed:  # dir
                for child_path in entry_removed['children']:
                    self.__remove_child(child_path)

            parent = os.path.dirname(path)
            if parent and parent in self.state:
                self.state[parent]['children'].discard(path)

        # Due to empty folder policy, we need to remove not
        # found entry (they already have been popped)

        if os.path.lexists(fpath):  # we want to catch broken symlink too
            if os.path.isfile(fpath):
                os.remove(fpath)
            elif os.path.islink(fpath):
                os.remove(fpath)
            else:
                os.removedirs(fpath)

    def __children(self, entry):
        """Compute the children of the current entry.

        Args:
            entry: the current entry metadata of self.path.

        Yields:
            The children's hashes if it has one
            If it does not, it's an empty directory

        """
        for path in entry['children']:
            h = self.state.get(path)
            if not h:
                continue
            c = h.get('checksums')
            if not c:
                continue
            yield c

    def __update_checksum(self):
        """Update the root path self.path's checksums according to the
        children's hashes.

        This function is expected to be called when the folder has
        been completely 'walked'.

        """
        d = self.state.get(self.path, default_dictionary())
        # Retrieve the list of the current folder's children hashes
        ls_hashes = list(self.__children(d))
        if ls_hashes:
            d['checksums'] = git._compute_tree_metadata(self.path, ls_hashes)
            self.state[self.path] = d
        else:   # To compute with empty directories, remove the else
                # and use ls_hashes even if empty
            self.__remove_child(self.path)

    def open_directory(self, *args):
        """Updating existing directory.

        """
        path = args[0].encode('utf-8')
        self.__add_child(path)
        return SWHDirEditor(self.state, self.rootpath, path=path)

    def add_directory(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """Adding a new directory.

        """
        path = path.encode('utf-8')
        self.__add_child(path)
        return SWHDirEditor(self.state, rootpath=self.rootpath, path=path)

    def open_file(self, *args):
        """Updating existing file.

        """
        path = args[0].encode('utf-8')
        self.__add_child(path)
        return SWHFileEditor(self.state, rootpath=self.rootpath, path=path)

    def add_file(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """Creating a new file.

        """
        path = path.encode('utf-8')
        self.__add_child(path)
        return SWHFileEditor(self.state, rootpath=self.rootpath, path=path)

    def change_prop(self, key, value):
        pass

    def delete_entry(self, path, revision):
        """Remove a path.

        """
        self.__remove_child(path.encode('utf-8'))

    def close(self):
        """Function called when we finish walking a repository.

        """
        self.__update_checksum()


class SWHEditor:
    """Base class in charge of receiving events.

    """
    __slots__ = ['rootpath', 'state']

    def __init__(self, rootpath, state):
        self.rootpath = rootpath
        self.state = state

    def set_target_revision(self, revnum):
        pass

    def abort(self):
        pass

    def close(self):
        pass

    def open_root(self, base_revnum):
        return SWHDirEditor(self.state,
                            rootpath=self.rootpath,
                            path=b'/')


def compute_or_update_hash_from_replay_at(conn, rev, rootpath, state):
    """Given a connection to the svn server, a revision, a rootpath and a
    hash state, compute the hash updated from a replay for that
    revision.

    Args:
        conn: The connection object to the remove svn repository
        rev: The revision to play the replay.
        rootpath: the root from which computation takes place
        state: the current state to update

    Returns:
        The updated state
        Beware that the rootpath has been changed on disk as well.

    """
    editor = SWHEditor(state=state, rootpath=rootpath)
    conn.replay(rev, rev+1, editor)
    state = editor.state
    # When accepting empty folder, this should be removed
    if not state:  # dangling tree at root
        # hack: empty tree at level 1: `git hash-object -t tree /dev/null`
        state[b'/'] = {
            'checksums': {
                'sha1_git': hex_to_hash(
                    '4b825dc642cb6eb9a060e54bf8d69288fbee4904'),
                'path': rootpath
            },
            'children': set()
        }

    return state


@click.command()
@click.option('--local-url', default='/tmp',
              help="local svn working copy")
@click.option('--svn-url', default='file:///home/storage/svn/repos/pkg-fox',
              help="svn repository's url.")
@click.option('--revision-start', default=1, type=click.INT,
              help="svn repository's starting revision.")
@click.option('--revision-end', default=-1, type=click.INT,
              help="svn repository's ending revision.")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode.")
@click.option('--cleanup/--nocleanup', default=True,
              help="Indicates whether to cleanup disk when done or not.")
def main(local_url, svn_url, revision_start, revision_end, debug, cleanup):
    conn = RemoteAccess(svn_url.encode('utf-8'),
                        auth=Auth([get_username_provider()]))

    os.makedirs(local_url, exist_ok=True)

    rootpath = tempfile.mkdtemp(prefix=local_url,
                                suffix='-'+os.path.basename(svn_url))

    rootpath = rootpath.encode('utf-8')

    # Do not go beyond the repository's latest revision
    revision_end_max = conn.get_latest_revnum()
    if revision_end == -1:
        revision_end = revision_end_max

    revision_end = min(revision_end, revision_end_max)

    try:
        state = {}
        for r in range(revision_start, revision_end+1):
            state = compute_or_update_hash_from_replay_at(conn,
                                                          r,
                                                          rootpath,
                                                          state)
            print('r%s %s' % (r, hashutil.hash_to_hex(
                state[b'/']['checksums']['sha1_git'])))

        if debug:
            print('%s' % rootpath.decode('utf-8'))
    finally:
        if cleanup:
            if os.path.exists(rootpath):
                shutil.rmtree(rootpath)

if __name__ == '__main__':
    main()
