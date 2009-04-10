import os
import os.path
import re
from nose import with_setup

from tsdb import *
from tsdb.row import *
from tsdb.chunk_locator import *

TEST_DB = "tmp/chunk_locator"

def db_reset():
    os.system("rm -rf %s %s" % (TEST_DB, TEST_DB + "_alt"))

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_noop():
    TSDB.create(TEST_DB)
    db = TSDB(TEST_DB, chunk_prefixes=[TEST_DB + "_alt", TEST_DB])

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    assert v.chunks['19700101'].path == os.path.join(TEST_DB, 'bar', '19700101')

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_resolver():
    def resolve(db, chunk_path):
        """puts YYYYMMDD chunks in prefix[0], else chunk_path"""
        relpath = db.get_relpath(chunk_path)
        chunk = os.path.basename(chunk_path)
        if re.search('^\d{8}$', chunk):
            newpath = "/".join([db.chunk_prefixes[0], relpath])
            newdir = os.path.dirname(newpath)
            if not os.path.isdir(newdir):
                os.makedirs(newdir)
            return newpath
        else:
            return chunk_path

    TSDB.create(TEST_DB)
    db = TSDB(TEST_DB, chunk_prefixes=[TEST_DB + "_alt", TEST_DB],
            new_chunk_resolver=resolve)

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    assert v.chunks['19700101'].path == os.path.join(TEST_DB + "_alt", 'bar', '19700101')

    v = db.get_var("bar")
    assert v.chunks['19700101'].path == os.path.join(TEST_DB + "_alt", 'bar', '19700101')

    del db.vars['bar']

    os.system("mv %s_alt/bar/19700101 %s/bar/19700101" % (TEST_DB, TEST_DB))
    v = db.get_var("bar")
    l = [d for d in v.select(begin=0,end=1)]
    assert v.chunks['19700101'].path == os.path.join(TEST_DB, 'bar', '19700101')

