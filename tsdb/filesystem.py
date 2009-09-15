#!/usr/bin/env python

from fs.base import FS
import fs.osfs
import fs.multifs

def get_fs(root, prefixes):
    """return an FS object suitable for the environment"""
    rootfs = fs.osfs.OSFS(root, thread_syncronize=False)

    if not prefixes:
        return rootfs

    unionfs = UnionFS()

    unionfs.addfs("root", rootfs)
    i = 0
    for prefix in prefixes:
        name = "prefix%d" % i
        prefix_fs = fs.osfs.OSFS(prefix)
        unionfs.addfs(name, prefix_fs)
        i += 1

    return unionfs
    
class UnionFS(fs.multifs.MultiFS):
    """Synthetic union file system.
    
    ``UnionFS`` creates a union filesystem by searching each subfilesystem in
    order to find files.  New files are always created in the first
    subfilesystem.

    This is particularly useful in a situation where there is smaller, fast
    filesystem (eg. in RAM or on an SSD) for frequently accessed data and a
    larger, slower filesystem (eg. SATA or NFS) for less frequently accessed
    data.

    UnionFS does not handle the migration of data from one layer to the other.
    """

    def __init__(self):
        FS.__init__(self, thread_syncronize=False)

        self.fs_sequence = []
        self.fs_lookup = {}

    def makedir(self, path, **kwargs):
        fs = self._delegate_search('/')
        return fs.makedir(path, **kwargs)

    def open(self, path, mode="r", **kwargs):
        if not self.exists(path) and mode in ('w', 'r+', 'w+', 'a+'):
            fs = self._delegate_search('/')
            return fs.open(path, mode=mode, **kwargs)
        else:
            return super(UnionFS, self).open(path, mode=mode, **kwargs)


