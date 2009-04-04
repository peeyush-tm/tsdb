#!/usr/bin/env python

import os

from nose import with_setup

from tsdb import *
from tsdb.row import Counter64, TimeTicks
from tsdb.chunk_mapper import YYYYMMDDChunkMapper

TESTDB = os.path.join(os.environ.get('TMPDIR', 'tmp'), 'actual_testdb')

def db_reset():
    os.system("rm -rf %s" % TESTDB)

@with_setup(db_reset, db_reset)
def test_rounding1():
    """This caused a rounding error in one version of the code.
    From observed data."""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'],
            {'HEARTBEAT': 90})
    var.insert(Counter64(1204329701, ROW_VALID, 54652476))
    var.insert(Counter64(1204329731, ROW_VALID, 54652612))
    var.update_all_aggregates()

@with_setup(db_reset, db_reset)
def test_erroneous_data1():
    """value went backwards but was not a rollover.
    From observed data."""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    up = db.add_var("uptime", TimeTicks, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'],
            {'HEARTBEAT': 90})

    var.insert(Counter64(1204345906, ROW_VALID, 54697031))
    var.insert(Counter64(1204345937, ROW_VALID, 54696971))
    var.insert(Counter64(1204345967, ROW_VALID, 54696981))

    up.insert(TimeTicks(1204345906, ROW_VALID, 677744266))
    up.insert(TimeTicks(1204345937, ROW_VALID, 677747340))
    var.update_all_aggregates(uptime_var=up)

@with_setup(db_reset, db_reset)
def test_gaps1():
    """there are one or more missing chunks in the middle of the range"""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    up = db.add_var("uptime", TimeTicks, 30, YYYYMMDDChunkMapper)

    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(1 + 2*24*3600, ROW_VALID, 1))
    var.flush()

    #os.system("ls -l %s/test1" % TESTDB)
    var.get(1 + 24*3600)


@with_setup(db_reset, db_reset)
def test_select_bounds():
    """if select gets called with a begin time that isn't on a slot boundary
    data may not be found in the last slot."""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(33, ROW_VALID, 2))
    var.flush()

    l = [x for x in  var.select(begin=5)]
    print l
    assert len(l) == 2

@with_setup(db_reset, db_reset)
def test_select_bounds2():
    """select returns one row too many"""

    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(33, ROW_VALID, 2))
    var.flush()

    l = [x for x in var.select(begin=5, end=30)]
    print l
    assert len(l) == 1

def create_inclusive_test_data_set():
    db = TSDB.create(TESTDB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    var.insert(Counter64(0, ROW_VALID, 1))
    var.insert(Counter64(33, ROW_VALID, 2))
    var.flush()
    return var

@with_setup(db_reset, db_reset)
def test_select_inclusive_end():
    """check for inclusive results at end of time range"""

    var = create_inclusive_test_data_set()

    l = [x for x in var.select(begin=33, end=33)]
    print l
    assert len(l) == 1
    assert l[0].timestamp == 33

@with_setup(db_reset, db_reset)
def test_select_inclusive_begin():
    """check for inclusive results at end of time range"""

    var = create_inclusive_test_data_set()

    l = [x for x in var.select(begin=0, end=0)]
    print l
    assert len(l) == 1
    assert l[0].timestamp == 0

@with_setup(db_reset, db_reset)
def test_select_inclusive_begin():
    """check for inclusive results at end of time range"""

    var = create_inclusive_test_data_set()

    l = [x for x in var.select(begin=0, end=0)]
    print l
    assert len(l) == 1
    assert l[0].timestamp == 0
