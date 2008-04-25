"""Run tests against RRD data"""
import os
import random
import math
from nose import with_setup

from tsdb import *
from tsdb.row import Counter32, Counter64
from tsdb.chunk_mapper import YYYYMMDDChunkMapper

import rrdtool

from fpconst import isNaN

TEST_DB = "tmp/rrd_test"
TEST_RRD_DB_DIR = "tmp/rrd_test_rrds"

def setup_run():
    os.system("rm -rf %s %s" % (TEST_DB, TEST_RRD_DB_DIR))
    db = TSDB.create(TEST_DB)
    os.mkdir(TEST_RRD_DB_DIR)
   
def rrd_from_tsdb_var(var, begin, heartbeat=None):
    name = os.path.basename(var.path)
    step = var.metadata['STEP']
    rrd_file = os.path.join(TEST_RRD_DB_DIR, "%s.rrd" % name)
    rrd_args = [rrd_file, "--start", str(begin), "--step", str(step)]

    if var.type in (Counter32, Counter64):
        if heartbeat is None:
            heartbeat = step*2
        rrd_args.append("DS:%s:COUNTER:%d:0:U" % (name, heartbeat))

    for aggname in var.list_aggregates():
        agg = var.get_aggregate(aggname)
        for agg_func in agg.metadata['AGGREGATES']:
            if agg_func != 'delta':
                rrd_args.append("RRA:%s:0.5:%d:100000" % (agg_func.upper(),
                    agg.metadata['STEP'] / step))

    print rrd_args
    rrdtool.create(*rrd_args)

    return rrd_file

@with_setup(setup_run, None)
def test_simple_rrd():
    """Compare random data with RRDTool for several aggregates."""
    db = TSDB(TEST_DB)
    var = db.add_var("foo", Counter32, 60, YYYYMMDDChunkMapper)
    var.add_aggregate("60s", 60, YYYYMMDDChunkMapper, ["average", "delta"],
            metadata=dict(HEARTBEAT=120) )
    var.add_aggregate("120s", 120, YYYYMMDDChunkMapper, ["average", "delta"])
    var.add_aggregate("10m", 10*60, YYYYMMDDChunkMapper, ["average", "delta"])

    begin = 3600*24*365*20
    rrd_file = rrd_from_tsdb_var(var, begin-60)

    v = 0
    last_v = 0
    for t in range(begin, begin+60*60*24, 60):
        z = random.randint(0,59)
        v += random.randint(0, 2**20)
        var.insert(Counter32(t+z, ROW_VALID, v))
        u = "%s:%s" % (t+z, v)
        rrdtool.update(rrd_file,u)

        last_v = v

    var.update_all_aggregates()

    def compare_aggs(aggname):
        step = int(aggname)
        skip = step
        agg = var.get_aggregate(aggname)
        for t in range(begin+skip, begin+60*60*24 - skip, step):
            args = [rrd_file, "AVERAGE", "-r", aggname, "-s", str(t), "-e", str(t)]
            rrd_out = rrdtool.fetch(*args)
            rrd_val = rrd_out[-1][0][0]
            tsdb_val = agg.get(t).average
            if rrd_val is None and tsdb_val == 0.0:
                continue
    
            try:
                same = math.fabs(rrd_val - tsdb_val) < 0.1
            except:
                same = False

            print same, t, rrd_val, tsdb_val, agg.get(t).delta#, rrd_out
            assert same #rrd_val == tsdb_val

    for aggname in ("60", "120", "600"):
        print "checking %s" % aggname
        compare_aggs(aggname)


@with_setup(setup_run, None)
def test_rrd_gap1():
    """Test that we handle gaps in a similar fashion to RRDTool.

    We aren't identical though because we will optimistically report partial
    results for the last timestep.  RRDTool keeps some state to make sure
    it's got full data for a timestep before reporting anything.
    """
    db = TSDB(TEST_DB)
    var = db.add_var("foo", Counter32, 30, YYYYMMDDChunkMapper)
    var.add_aggregate("30s", 30, YYYYMMDDChunkMapper, ["average", "delta"],
            metadata=dict(HEARTBEAT=90) )
    begin = 3600*24*365*20
    rrd_file = rrd_from_tsdb_var(var, begin-60, heartbeat=90)

    data = (
            (0, 0),
            # miss poll in slot 30, gap smaller than heartbeat
            (75, 75000),
            # miss poll at slots 90 and 120, gap larger than heartbeat
            (166, 166000),
            (195, 195000),
            (225, 225000),
            )
    for (t,v) in data:
        var.insert(Counter32(begin+t, ROW_VALID, v))
        rrdtool.update(rrd_file, "%d:%d" % (begin+t,v))

    var.update_all_aggregates()

    agg = var.get_aggregate("30s")
    for t in range(0, 210, 30):
        t += begin
        args = [rrd_file, "AVERAGE", "-r 30", "-s", str(t), "-e", str(t)]
        a = agg.get(t)
        r = rrdtool.fetch(*args)[-1][0][0]
        print t-begin, a, r
        if r is None and isNaN(a.average):
            assert True
        else:
            assert a.average == r

def teardown():
    pass
