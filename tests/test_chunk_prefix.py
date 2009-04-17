import os
import os.path
import re
from nose import with_setup

from tsdb import *
from tsdb.row import *

TEST_DB = "tmp/chunk_locator"
prefixes=[TEST_DB + "_alt", TEST_DB]

def db_reset():
    os.system("rm -rf %s %s" % (TEST_DB, TEST_DB + "_alt"))

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_noop():
    TSDB.create(TEST_DB, chunk_prefixes=prefixes)
    db = TSDB(TEST_DB)

    v = db.add_var("bar", Counter32, 60, chunk_mapper.YYYYMMDDChunkMapper)
    v.insert(Counter32(0, 0, 0))
    assert v.chunks['19700101'].path == os.path.join(TEST_DB, 'bar', '19700101')

    print db.metadata['CHUNK_PREFIXES'], prefixes
    assert len(db.metadata['CHUNK_PREFIXES']) == len(prefixes)
    for i in range(len(prefixes)):
        assert db.metadata['CHUNK_PREFIXES'][i] == prefixes[i]

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

@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_resolver():

    resolve_path = ".".join((__name__, resolve.__name__))

    TSDB.create(TEST_DB, chunk_prefixes=[TEST_DB + "_alt", TEST_DB],
            new_chunk_resolver=resolve_path)
    db = TSDB(TEST_DB)

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

    v.insert(Counter32(24*60*60 + 1, 1, 1))
    v.get(24*60*60 + 1)
    assert v.chunks['19700102'].path == os.path.join(TEST_DB + "_alt", 'bar', '19700102')

    print v.all_chunks()
    assert v.all_chunks() == ['19700101', '19700102']


@with_setup(db_reset, None) #db_reset)
def test_PrefixChunkLocator_undef_function():
    try:
        TSDB.create(TEST_DB, chunk_prefixes=prefixes,
            new_chunk_resolver="quux.bar.quux.foo")
    except TSDBError:
        assert True
    else:
        assert False

