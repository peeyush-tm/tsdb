"""Run tests against RRD data"""
import os
import random
import math
from nose import with_setup
from nose.plugins.skip import SkipTest

from tsdb import *
from tsdb.row import Counter32, Counter64
from tsdb.chunk_mapper import YYYYMMDDChunkMapper
from tsdb.util import rrd_from_tsdb_var

try:
    import rrdtool
except ImportError:
    raise SkipTest()

from fpconst import isNaN

TESTDB = os.path.join(os.environ.get('TMPDIR', 'tmp'), 'rrdtest')
TESTRRD = os.path.join(os.environ.get('TMPDIR', 'tmp'), 'rrdtest_rrds')

def setup_run():
    os.system("rm -rf %s %s" % (TESTDB, TESTRRD))
    db = TSDB.create(TESTDB)
    os.mkdir(TESTRRD)
  
def make_rrd(*args, **kwargs):
    print args, kwargs
    rrd_args = rrd_from_tsdb_var(*args, **kwargs)
    print rrd_args
    rrdtool.create(*rrd_args)
    return rrd_args[0]

@with_setup(setup_run, None)
def test_simple_rrd():
    """Compare random data with RRDTool for several aggregates."""
    db = TSDB(TESTDB)
    var = db.add_var("foo", Counter32, 60, YYYYMMDDChunkMapper)
    var.add_aggregate("60s", YYYYMMDDChunkMapper, ["average", "delta"],
            metadata=dict(HEARTBEAT=120) )
    var.add_aggregate("120s", YYYYMMDDChunkMapper, ["average", "delta"])
    var.add_aggregate("10m", YYYYMMDDChunkMapper, ["average", "delta"])

    begin = 3600*24*365*20
    rrd_file = make_rrd(var, begin-60, TESTRRD, rows=10000)

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
        skip = step*2
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
    db = TSDB(TESTDB)
    var = db.add_var("foo", Counter32, 30, YYYYMMDDChunkMapper)
    var.add_aggregate("30s", YYYYMMDDChunkMapper, ["average", "delta"],
            metadata=dict(HEARTBEAT=90) )
    begin = 3600*24*365*20
    rrd_file = make_rrd(var, begin-60, TESTRRD, heartbeat=90)

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
        u = "%d:%d" % (begin+t,v)
        rrdtool.update(rrd_file, u)
        print u

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
        elif t-begin == 150 and a.average == 1000 and r == None:
            # RRD takes one step longer to recover
            assert True
        else:
            assert a.average == r

def teardown():
    pass
