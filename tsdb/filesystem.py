#!/usr/bin/env python

import os
import os.path
import errno

class OSFS(object):
    def __init__(self, root):
        self.root = root

    def __str__(self):
        return '<OSFS: %s>' % self.root

    def __repr__(self):
        return '<OSFS: %s>' % self.root

    def resolve_path(self, path):
        if path[0] == '/':
            path = path[1:]
        return os.path.abspath(os.path.join(self.root, path))

    def exists(self, path):
        return os.path.exists(self.resolve_path(path))

    def isdir(self, path):
        return os.path.isdir(self.resolve_path(path))

    def isfile(self, path):
        return os.path.isfile(self.resolve_path(path))

    def getsize(self, path):
        return os.path.getsize(self.resolve_path(path))

    def makedir(self, path):
        return os.mkdir(self.resolve_path(path))

    def makedirs(self, path):
        return os.makedirs(self.resolve_path(path))

    def listdir(self, path):
        return os.listdir(self.resolve_path(path))

    def open(self, path, mode="r", **kwargs):
        return open(self.resolve_path(path), mode, **kwargs)

def get_fs(root, prefixes):
    """return an FS object suitable for the environment"""
    rootfs = OSFS(root)

    if not prefixes:
        return rootfs

    unionfs = UnionFS()
    unionfs.addfs(rootfs)

    for prefix in prefixes:
        unionfs.addfs(OSFS(prefix))

    return unionfs
    
class UnionFS(object):
    """Synthetic union file system.
    
    ``UnionFS`` creates a union filesystem by searching each subfilesystem in
    order to find files.  New files and directories are always created in the
    first subfilesystem.

    This is particularly useful in a situation where there is smaller, fast
    filesystem (eg. in RAM or on an SSD) for frequently accessed data and a
    larger, slower filesystem (eg. SATA or NFS) for less frequently accessed
    data.

    UnionFS does not handle the migration of data from one layer to the other.
    """

    def __init__(self):
        self.fs_sequence = []

    def _search(self, path):
        for fs in self.fs_sequence:
            if fs.exists(path):
                return fs

        return None

    def _not_found(self, path):
        e = IOError()
        e.filename = path
        e.errno = errno.ENOENT
        e.strerror = os.strerror(errno.ENOENT)
        return e

    def resolve_path(self, path):
        fs = self._search(path)
        if not fs:
            raise self._not_found(path)

        return fs.resolve_path(path)

    def addfs(self, fs):
        self.fs_sequence.append(fs)

    def exists(self, path):
        return self._search(path) is not None

    def isdir(self, path):
        fs = self._search(path)
        if fs:
            if os.path.isdir(fs.resolve_path(path)):
                return True

        return False

    def isfile(self, path):
        fs = self._search(path)
        if fs:
            if os.path.isfile(fs.resolve_path(path)):
                return True

        return False

    def getsize(self, path):
        return os.path.getsize(self.resolve_path(path))

    def listdir(self, path):
        files = []
        notfound_cnt = 0
        for fs in self.fs_sequence:
            try:
                files += fs.listdir(path)
            except IOError:
                pass
            except OSError, e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    notfound_cnt += 1

        if notfound_cnt == len(self.fs_sequence):
            raise self._not_found(path)

        return list(set(files))

    def makedir(self, path, **kwargs):
        fs = self.fs_sequence[0]
        return fs.makedir(path, **kwargs)

    def open(self, path, mode="r", **kwargs):
        if not self.exists(path) and mode in ('w', 'r+', 'w+', 'a+'):
            fs = self.fs_sequence[0]
            dir = os.path.dirname(path)
            if not fs.exists(dir):
                # the directory structure may exist in a backing store, but if
                # we're creating a file we need it to exist in the top layer
                fs.makedirs(dir)
            return fs.open(path, mode=mode, **kwargs)
        else:
            for fs in self.fs_sequence:
                if fs.exists(path):
                    return open(fs.resolve_path(path), mode=mode, **kwargs)

            raise self._not_found(path)
