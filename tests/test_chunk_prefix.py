import os
import os.path
import re
from nose import with_setup

from tsdb import *
from tsdb.row import *

TEST_DB = "tmp/chunk_locator"
prefixes=[TEST_DB + "_alt"]

def db_reset():
    os.system("rm -rf %s %s" % (TEST_DB, " ".join(prefixes)))
    for prefix in prefixes:
        os.makedirs(prefix)
    
def check_path(fs, a, b):
    pa = fs.getsyspath(a)
    pb = os.path.abspath(b)
    print pa, pb
    assert pa == pb

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_noop():
    TSDB.create(TEST_DB, chunk_prefixes=prefixes)
    db = TSDB(TEST_DB)

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    check_path(v.fs, v.chunks['19700101'].path, 
                    os.path.join(TEST_DB, 'bar', '19700101'))

    print db.metadata['CHUNK_PREFIXES'], prefixes
    assert len(db.metadata['CHUNK_PREFIXES']) == len(prefixes)
    for i in range(len(prefixes)):
        assert db.metadata['CHUNK_PREFIXES'][i] == prefixes[i]

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_create():
    TSDB.create(TEST_DB, chunk_prefixes=[TEST_DB + "_alt", TEST_DB])
    db = TSDB(TEST_DB)

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    check_path(v.fs, v.chunks['19700101'].path,
            os.path.join(TEST_DB, 'bar', '19700101'))

    v = db.get_var("bar")
    check_path(v.fs, v.chunks['19700101'].path,
        os.path.join(TEST_DB, 'bar', '19700101'))

    del db.vars['bar']

    os.mkdir("%s/bar" % (TEST_DB+'_alt'))
    os.system("mv %s/bar/19700101 %s/bar/19700101" % (TEST_DB, TEST_DB+'_alt'))
    v = db.get_var("bar")
    l = [d for d in v.select(begin=0,end=1)]
    check_path(v.fs, v.chunks['19700101'].path,
        os.path.join(TEST_DB+'_alt', 'bar', '19700101'))

    v.insert(Counter32(24*60*60 + 1, 1, 1))
    v.get(24*60*60 + 1)
    check_path(v.fs, v.chunks['19700102'].path,
            os.path.join(TEST_DB, 'bar', '19700102'))

    print v.all_chunks()
    assert v.all_chunks() == ['19700101', '19700102']


