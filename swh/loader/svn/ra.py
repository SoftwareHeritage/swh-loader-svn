# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Remote Access client to svn server.

"""

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


def apply_txdelta_handler(sbuf, target_stream):
    """Return a function that can be called repeatedly with txdelta windows.
    When done, closes the target_stream.

    Adapted from subvertpy.delta.apply_txdelta_handler to close the
    stream when done.

    Args
        sbuf: Source buffer
        target_stream: Target stream to write to.

    Returns
         Function to be called to apply txdelta windows

    """
    def apply_window(window):
        if window is None:
            target_stream.close()
            return  # Last call
        patch = delta.apply_txdelta_window(sbuf, window)
        target_stream.write(patch)
    return apply_window


class SWHFileEditor:
    """File Editor in charge of updating file on disk and memory objects.

    """
    __slots__ = ['objects', 'path', 'fullpath', 'executable', 'link',
                 'convert_eol']

    def __init__(self, objects, rootpath, path):
        self.objects = objects
        self.path = path
        # default value: 0, 1: set the flag, 2: remove the exec flag
        self.executable = 0
        self.link = None
        self.fullpath = os.path.join(rootpath, path)
        self.convert_eol = False

    def change_prop(self, key, value):
        if key == properties.PROP_EXECUTABLE:
            if value is None:  # bit flip off
                self.executable = 2
            else:
                self.executable = 1
        elif key == properties.PROP_SPECIAL:
            self.link = True
        elif key == 'svn:eol-style' and value in {'LF', 'native'}:
            self.convert_eol = True

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
                with open(self.fullpath, 'rb') as f:
                    sbuf = f.read()
        else:
            sbuf = b''

        t = open(self.fullpath, 'wb')
        return apply_txdelta_handler(sbuf, target_stream=t)

    def close(self):
        """When done with the file, this is called.
        So the file exists and is updated, we can:
        - adapt accordingly its execution flag if any
        - compute the objects' checksums

        """
        if self.link:
            filetype, source_link = self.__make_symlink()
            self.objects[self.path] = {
                'checksums': compute_svn_link_metadata(self.fullpath,
                                                       filetype=filetype,
                                                       data=source_link)
            }
            return

        if self.executable == 1:
            os.chmod(self.fullpath, 0o755)
        elif self.executable == 2:
            os.chmod(self.fullpath, 0o644)

        if self.convert_eol:
            with open(self.fullpath, 'rb') as f:
                raw = f.read()
            with open(self.fullpath, 'wb') as f:
                f.write(raw.replace(b'\r', b''))

        # And now compute file's checksums
        self.objects[self.path] = {
            'checksums': git.compute_blob_metadata(self.fullpath)
        }


def default_dictionary():
    """Default dictionary.

    """
    return dict(checksums=None, children=set())


class BaseDirSWHEditor:
    """Base class implementation of dir editor.

    cf. SWHDirEditor for an implementation that hashes every directory
    encountered.

    cf. SWHDirEditorNoEmptyFolder for an implementation that deletes
    empty folder

    Instantiate a new class inheriting frmo this class and define the
    following function:

    - def update_checksum(self):
        Compute the checksums at current state
    - def open_directory(self, *args):
        Update an existing folder.
    - def add_directory(self, *args):
        Add a new one.

    """
    __slots__ = ['objects', 'rootpath', 'path']

    def __init__(self, objects, rootpath, path):
        self.objects = objects
        self.rootpath = rootpath
        self.path = path
        # build directory on init
        os.makedirs(os.path.join(rootpath, path), exist_ok=True)

    def add_child(self, path):
        """Add a children path to the actual objects for the current directory
        seen as the parent.

        Args:
            path: The child to add

        """
        d = self.objects.get(self.path, default_dictionary())
        d['children'].add(path)
        self.objects[self.path] = d

    def remove_child(self, path):
        """Remove a path from the current objects.

        The path can be resolved as link, file or directory.

        This function takes also care of removing the link between the
        child and the parent.

        Args:
            path: to remove from the current objects.

        """
        entry_removed = self.objects.pop(path, None)
        fpath = os.path.join(self.rootpath, path)
        if entry_removed:
            if 'children' in entry_removed:  # dir
                for child_path in entry_removed['children']:
                    self.remove_child(child_path)

            parent = os.path.dirname(path)
            if parent and parent in self.objects:
                self.objects[parent]['children'].discard(path)

        if os.path.lexists(fpath):  # we want to catch broken symlink too
            if os.path.isfile(fpath):
                os.remove(fpath)
            elif os.path.islink(fpath):
                os.remove(fpath)
            else:
                shutil.rmtree(fpath)

    def update_checksum(self):
        raise NotImplementedError('This should be implemented.')

    def open_directory(self, *args):
        raise NotImplementedError('This should be implemented.')

    def add_directory(self, *args):
        raise NotImplementedError('This should be implemented.')

    def open_file(self, *args):
        """Updating existing file.

        """
        path = args[0].encode('utf-8')
        self.add_child(path)
        return SWHFileEditor(self.objects, rootpath=self.rootpath, path=path)

    def add_file(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """Creating a new file.

        """
        path = path.encode('utf-8')
        self.add_child(path)
        return SWHFileEditor(self.objects, rootpath=self.rootpath, path=path)

    def change_prop(self, key, value):
        pass

    def delete_entry(self, path, revision):
        """Remove a path.

        """
        self.remove_child(path.encode('utf-8'))

    def close(self):
        """Function called when we finish walking a repository.

        """
        self.update_checksum()


class SWHDirEditor(BaseDirSWHEditor):
    """Directory Editor in charge of updating directory hashes computation.

    This implementation includes empty folder in the hash computation.

    """
    def update_checksum(self):
        """Update the root path self.path's checksums according to the
        children's objects.

        This function is expected to be called when the folder has
        been completely 'walked'.

        """
        d = self.objects.get(self.path, default_dictionary())
        # Retrieve the list of the current folder's children objects
        ls_hashes = list(git.children_hashes(d['children'],
                                             objects=self.objects))
        d['checksums'] = git._compute_tree_metadata(self.path, ls_hashes)
        self.objects[self.path] = d

    def open_directory(self, *args):
        """Updating existing directory.

        """
        path = args[0].encode('utf-8')
        self.add_child(path)
        return SWHDirEditor(self.objects, self.rootpath, path=path)

    def add_directory(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """Adding a new directory.

        """
        path = path.encode('utf-8')
        self.add_child(path)
        return SWHDirEditor(self.objects, rootpath=self.rootpath, path=path)


class SWHDirEditorNoEmptyFolder(BaseDirSWHEditor):
    """Directory Editor in charge of updating directory objects computation.

    """
    def update_checksum(self):
        """Update the root path self.path's checksums according to the
        children's objects.

        This function is expected to be called when the folder has
        been completely 'walked'.

        """
        d = self.objects.get(self.path, default_dictionary())
        # Retrieve the list of the current folder's children objects
        ls_hashes = list(git.children_hashes(d['children'],
                                             objects=self.objects))
        if ls_hashes:
            d['checksums'] = git._compute_tree_metadata(self.path, ls_hashes)
            self.objects[self.path] = d
        else:   # To compute with empty directories, remove the else
            # and use ls_hashes even if empty
            self.remove_child(self.path)

    def open_directory(self, *args):
        """Updating existing directory.

        """
        path = args[0].encode('utf-8')
        self.add_child(path)
        return SWHDirEditorNoEmptyFolder(self.objects, self.rootpath,
                                         path=path)

    def add_directory(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """Adding a new directory.

        """
        path = path.encode('utf-8')
        self.add_child(path)
        return SWHDirEditorNoEmptyFolder(self.objects,
                                         rootpath=self.rootpath,
                                         path=path)


class BaseSWHEditor:
    """SWH Base class editor in charge of receiving events.

    """
    def __init__(self, rootpath, objects):
        self.rootpath = rootpath
        self.objects = objects

    def set_target_revision(self, revnum):
        pass

    def abort(self):
        pass

    def close(self):
        pass

    def open_root(self, base_revnum):
        raise NotImplementedError('Instantiate an swh dir editor of your '
                                  ' choice depending of the hash computation '
                                  ' policy you want')


class SWHEditorNoEmptyFolder(BaseSWHEditor):
    """SWH Editor in charge of replaying svn events and computing objects
    hashes along.

    This implementation removes empty folders and do not account for
    them when computing objects hashes.

    """
    def open_root(self, base_revnum):
        return SWHDirEditorNoEmptyFolder(self.objects,
                                         rootpath=self.rootpath,
                                         path=b'')


class SWHEditor(BaseSWHEditor):
    """SWH Editor in charge of replaying svn events and computing objects
    along.

    This implementation accounts for empty folder during hash
    computations.

    """
    def open_root(self, base_revnum):
        return SWHDirEditor(self.objects, rootpath=self.rootpath, path=b'')


class BaseSWHReplay:
    """Base replay class.
    Their role is to compute objects for a particular revision.

    This class is intended to be inherited to:
    - initialize the editor (global loading policy depends on this editor)

    - override the compute_hashes function in charge of computing
    hashes between rev and rev+1

    cf. SWHReplayNoEmptyFolder and SWHReplay for instanciated classes.

    """
    def replay(self, rev):
        """Replay svn actions between rev and rev+1.

           This method updates in place the self.editor.objects's reference.
           This also updates in place the filesystem.

        Returns:
           The updated objects

        """
        self.conn.replay(rev, rev+1, self.editor)
        return self.editor.objects

    def compute_hashes(self, rev):
        """Compute hashes at revisions rev.
        Expects the objects to be at previous revision's objects.

        Args:
            rev: The revision to start the replay from.

        Returns:
            The updated objects between rev and rev+1.
            Beware that this mutates the filesystem at rootpath accordingly.

        """
        raise NotImplementedError('This should be overridden by subclass')


class SWHReplayNoEmptyFolder(BaseSWHReplay):
    """Replay class.

    This class computes objects hashes for all files and folders as
    long as those folders are not empty ones.

    If empty folder are discovered, they are removed from the
    filesystem and their hashes are not computed.

    """
    def __init__(self, conn, rootpath, objects=None):
        self.conn = conn
        self.rootpath = rootpath
        self.editor = SWHEditorNoEmptyFolder(
            rootpath=rootpath,
            objects=objects if objects else {})

    def compute_hashes(self, rev):
        """Compute hashes at revisions rev.
        Expects the state to be at previous revision's objects.

        Args:
            rev: The revision to start the replay from.

        Returns:
            The updated objects between rev and rev+1.
            Beware that this mutates the filesystem at rootpath accordingly.

        """
        objects = self.replay(rev)
        if not objects:  # dangling tree at root
            # hack: empty tree at level 1: `git hash-object -t tree /dev/null`
            objects[b''] = {
                'checksums': {
                    'sha1_git': hex_to_hash(
                        '4b825dc642cb6eb9a060e54bf8d69288fbee4904'),
                    'path': self.rootpath,
                    'type': git.GitType.TREE,
                    'perms': git.GitPerm.TREE
                },
                'children': set()
            }
            self.editor.objects = objects

        return objects


class SWHReplay(BaseSWHReplay):
    """Replay class.

    All folders and files are considered for hash computations.

    """
    def __init__(self, conn, rootpath, objects=None):
        self.conn = conn
        self.rootpath = rootpath
        self.editor = SWHEditor(rootpath=rootpath,
                                objects=objects if objects else {})

    def compute_hashes(self, rev):
        """Compute hashes at revisions rev.
        Expects the state to be at previous revision's objects.

        Args:
            rev: The revision to start the replay from.

        Returns:
            The updated objects between rev and rev+1.
            Beware that this mutates the filesystem at rootpath accordingly.

        """
        return self.replay(rev)


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
@click.option('--empty-folder/--noempty-folder', default=True,
              help="Do not account empty folder during hash computation.")
def main(local_url, svn_url, revision_start, revision_end, debug, cleanup,
         empty_folder):
    """Script to present how to use SWHReplay class.

    """
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
        if empty_folder:
            replay = SWHReplay(conn, rootpath)
        else:
            replay = SWHReplayNoEmptyFolder(conn, rootpath)

        for rev in range(revision_start, revision_end+1):
            objects = replay.compute_hashes(rev)
            print('r%s %s' % (rev, hashutil.hash_to_hex(
                objects[b'']['checksums']['sha1_git'])))

        if debug:
            print('%s' % rootpath.decode('utf-8'))
    finally:
        if cleanup:
            if os.path.exists(rootpath):
                shutil.rmtree(rootpath)


if __name__ == '__main__':
    main()
