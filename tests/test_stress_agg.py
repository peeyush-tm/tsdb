#!/usr/bin/env python

import os

from nose import with_setup

from tsdb import *
from tsdb.row import Counter64
from tsdb.chunk_mapper import YYYYMMDDChunkMapper

TEST_DB = "tmp/agg_stress_db"

def db_reset():
    os.system("rm -rf %s" % TEST_DB)

@with_setup(db_reset, db_reset)
def test_gap1():
    """Test how the raw aggregator deals with gaps."""

    db = TSDB.create(TEST_DB)
    var = db.add_var("test1", Counter64, 30, YYYYMMDDChunkMapper)
    agg = var.add_aggregate("30s", YYYYMMDDChunkMapper, ['average', 'delta'],
            {'HEARTBEAT': 90})
    var.insert(Counter64(0, ROW_VALID, 30000))
    # miss a poll at 30s
    var.insert(Counter64(75, ROW_VALID, 105000))
    var.insert(Counter64(105, ROW_VALID, 135000))
    var.update_all_aggregates()

    for row in var.select(end=90):
        print row

    for row in agg.select(end=90):
        print row

