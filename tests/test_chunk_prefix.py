import os
import os.path
import re
from nose import with_setup

from tsdb import *
from tsdb.row import *

TEST_DB = "tmp/chunk_locator"
PREFIXES=[TEST_DB, TEST_DB + "_alt"]

def db_reset():
    os.system("rm -rf %s %s" % (TEST_DB, " ".join(PREFIXES)))
    for prefix in PREFIXES:
        os.makedirs(prefix)
    TSDB.create(TEST_DB, chunk_prefixes=PREFIXES)
    
def check_path(fs, a, b):
    pa = fs.resolve_path(a)
    pb = os.path.abspath(b)
    print pa, pb
    assert pa == pb

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_noop():
    db = TSDB(TEST_DB)

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    check_path(v.fs, v.chunks['19700101'].path, 
                    os.path.join(TEST_DB, 'bar', '19700101'))

    print db.metadata['CHUNK_PREFIXES'], PREFIXES
    assert len(db.metadata['CHUNK_PREFIXES']) == len(PREFIXES)
    for i in range(len(PREFIXES)):
        assert db.metadata['CHUNK_PREFIXES'][i] == PREFIXES[i]

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_prefix_count():
    db = TSDB(TEST_DB)
    l = [x for x in db.fs.fs_sequence]
    print l
    assert len(l) == 2

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_create():
    db = TSDB(TEST_DB)

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    v.flush()

    check_path(v.fs, v.chunks['19700101'].path,
            os.path.join(TEST_DB, 'bar', '19700101'))

    v = db.get_var("bar")
    check_path(v.fs, v.chunks['19700101'].path,
        os.path.join(TEST_DB, 'bar', '19700101'))

    del db.vars['bar']

    print db.metadata['CHUNK_PREFIXES']
    os.mkdir("%s/bar" % (TEST_DB+'_alt'))
    os.system("mv %s/bar/19700101 %s/bar/19700101" % (TEST_DB, TEST_DB+'_alt'))
    v = db.get_var("bar")
    l = [d for d in v.select(begin=0,end=1)]
    check_path(v.fs, v.chunks['19700101'].path,
        os.path.join(TEST_DB+'_alt', 'bar', '19700101'))

    v.insert(Counter32(24*60*60 + 1, 1, 1))
    v.flush()
    v.get(24*60*60 + 1)
    check_path(v.fs, v.chunks['19700102'].path,
            os.path.join(TEST_DB, 'bar', '19700102'))

    print v.all_chunks()
    assert v.all_chunks() == ['19700101', '19700102']


@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_missing_top_dir_create():
    db = TSDB(TEST_DB)
    print "yeehaw!", TEST_DB

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    v.flush()

    os.system("mv %s/bar %s" % (TEST_DB, TEST_DB+'_alt'))
    
    print v.chunks['19700101'].path

    v = db.get_var("bar")
    v.insert(Counter32(3600*24 + 1, 0, 0))
    v.flush()

    check_path(v.fs, v.chunks['19700102'].path,
            os.path.join(TEST_DB, 'bar', '19700102'))



@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_missing_top_dir_listdir():
    db = TSDB(TEST_DB)
    print "yeehaw!", TEST_DB

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.add_aggregate("60", chunk_mapper.YYYYMMDDChunkMapper, ['average','delta'],
                metadata=dict(HEARTBEAT=12*60*60))
    v.insert(Counter32(0, 0, 0))
    v.flush()

    # this used to raise an exception before bugfix
    print v.list_aggregates()

