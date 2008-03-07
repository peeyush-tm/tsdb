import unittest
import os
import os.path
import time

from tsdb import *

TESTDB = "testdb"

# XXX add test for disjoint chunks
# XXX add tests for min/max_timestamp for vars

class TSDBTestCase(unittest.TestCase):
    def setUp(self):
        self.db = TSDB.create(TESTDB)

    def tearDown(self):
        os.system("rm -rf " + TESTDB)

class CreateTSDB(unittest.TestCase):
    def testCreate(self):
        """can we create a db?"""
        try:
            TSDB.create(TESTDB)
        except:
            self.fail("unable to create db")

        if not os.path.isdir(TESTDB):
            self.fail("directory doesn't exist")

        if not os.path.isfile(os.path.join(TESTDB,"TSDB")):
            self.fail("metadata file TSDB doesn't exist")

    def testRecreate(self):
        """does trying to create the same db again fail?"""
        TSDB.create(TESTDB)
        self.assertRaises(TSDBAlreadyExistsError, TSDB.create, TESTDB)

    def tearDown(self):
        os.system("rm -rf " + TESTDB)

class CreateTSDBSet(TSDBTestCase):
    def doCreate(self, name):
        try:
            self.db.add_set(name)
        except Exception, e:
            print e.__class__.__name__, e
            self.fail(e)

        if not os.path.isdir(os.path.join(TESTDB,name)):
            self.fail("directory doesn't exist")

        if not os.path.isfile(os.path.join(TESTDB, name, "TSDBSet")):
            self.fail("metadata file TSDBSet doesn't exist")

    def testCreate(self):
        """can we create a TSDBSet?"""
        self.doCreate("foo")

    def testPathCreate(self):
        """can we create TSDBSet hierarchy?"""
        self.doCreate("blort/foo/bar")

    def testRecreate(self):
        self.db.add_set("foo")
        self.assertRaises(TSDBNameInUseError, self.db.add_set, "foo")

class CreateTSDBVar(TSDBTestCase):
    def doCreate(self, name):
        try:
            self.db.add_var(name, Counter32, 60, YYYYMMDDChunkMapper)
        except Exception, e:
            self.fail(e)

        if not os.path.isdir(os.path.join(TESTDB, name)):
            self.fail("directory doesn't exist")

        if not os.path.isfile(os.path.join(TESTDB, name, "TSDBVar")):
            self.fail("metadata file TSDBVar doesn't exist")

    def testCreate(self):
        """can we create a TSDBVar?"""
        self.doCreate("bar")

    def testPathCreate(self):
        """can we create a TSDBVar inside a TSDBSet?"""
        self.doCreate("baz/foo/bar")

    def testRecreate(self):
        self.db.add_var("bar", Counter32, 60, YYYYMMDDChunkMapper)
        self.assertRaises(TSDBNameInUseError, self.db.add_var, "bar", Counter32, 60, YYYYMMDDChunkMapper)

    def testGetPath(self):
        self.db.add_var("foo/bar", Counter32, 60, YYYYMMDDChunkMapper)
        try:
            self.db.get_var("foo/bar")
        except Exception, e:
            self.fail(e)

    def testGetLongPath(self):
        self.db.add_var("blort/baz/foo/bar", Counter32, 60, YYYYMMDDChunkMapper)
        try:
            self.db.get_var("blort/baz/foo/bar")
        except Exception, e:
            self.fail(e)

    def testOverlappingPaths(self):
        self.db.add_var("blort/baz/foo/bar", Counter32, 60, YYYYMMDDChunkMapper)
        try:
            self.db.add_var("blort/baz/foo/bat", Counter32, 60, YYYYMMDDChunkMapper)
        except Exception, e:
            self.fail(e)

class TSDBVarTestCase(TSDBTestCase):
    def setUp(self):
        TSDBTestCase.setUp(self)
        self.db.add_var("blort", Counter32, 60, YYYYMMDDChunkMapper)
        self.v = self.db.get_var("blort")

class TestGetData(TSDBVarTestCase):
    def testGet(self):
        self.assertRaises(TSDBVarEmpty, self.v.get, 1)

class TestMetadata(TSDBVarTestCase):
    def testStep(self):
        self.assertEqual(self.v.metadata['STEP'], 60)

class TestCaching(TSDBVarTestCase):
    def testNoCaching(self):
        v = self.db.get_var("blort")
        v.insert(Counter32(time.time(), 0, 1))
        v.insert(Counter32(time.time()+(32*24*3600), 0, 1))

        self.assertEqual(len(v.chunks), 1)

    def testCaching(self):
        v = self.db.get_var("blort")
        v.cache_chunks=True
        v.insert(Counter32(time.time(), 0, 1))
        v.insert(Counter32(time.time()+(32*24*3600), 0, 1))

        self.assertEqual(len(v.chunks), 2)

class TestBounds(TSDBVarTestCase):
    def setUp(self):
        TSDBVarTestCase.setUp(self)
        self.ts = 36*3600
        self.v.insert(Counter32(self.ts, ROW_VALID, 37))

    def testBounds(self):
        """Test that min and max timestamp functions."""
        self.assertEqual(24*3600, self.v.min_timestamp())
        self.assertEqual(48*3600-1, self.v.max_timestamp())
        
        self.assertEqual(self.ts, self.v.min_valid_timestamp())
        self.assertEqual(self.ts, self.v.max_valid_timestamp())

        self.v.insert(Counter32(self.ts + 24*3600, ROW_VALID, 37))
        self.assertEqual(self.ts + 24*3600, self.v.max_valid_timestamp())

        self.v.insert(Counter32(self.ts - 24*3600, ROW_VALID, 37))
        self.assertEqual(self.ts - 24*3600, self.v.min_valid_timestamp())


class TestChunkList(TSDBVarTestCase):
    def setUp(self):
        TSDBVarTestCase.setUp(self)
        self.ts = 1188344425
        self.v.insert(Counter32(self.ts, 0, 1))

    def testOneChunk(self):
        self.assertEqual(len(self.v.all_chunks()), 1)

    def testTwoChunks(self):
        self.v.insert(Counter32(self.ts + 24*3600, 0, 1))

        self.assertEqual(len(self.v.all_chunks()), 2)

        chunks = self.v.all_chunks()
        chunks.sort()

        self.assertEqual(chunks[0], "20070828")
        self.assertEqual(chunks[1], "20070829")
    
class TestSelect(TSDBVarTestCase):
    def setUp(self):
        TSDBVarTestCase.setUp(self)
        self.l = range(0, 601, 60)
        self.vars = []
        for i in self.l:
            if i/60 % 2 == 0:
                flags = ROW_VALID
            else:
                flags = 0

            self.vars.append(Counter32(i, flags, self.l.index(i)))
            self.v.insert(self.vars[-1])

    def testSingleSelect(self):
        r = self.v.select(0,59)

        i = 0
        for x in r:
            self.assertEqual(x, self.vars[i])
            i += 1

    def testMultipleSelect(self):
        r = self.v.select(0,601)

        i = 0
        for x in r:
            self.assertEqual(self.vars[i], x)
            i += 1

    def testFlagSelect(self):
        r = self.v.select(0,599,flags=ROW_VALID)

        i = 0
        for x in r:
            self.assertEqual(self.vars[i*2], x)
            i += 1

class TestData(TSDBTestCase):
    ts = 1184863723
    step = 60

    def testData(self):
        """Test moderate datasets with each TSDBRow subclass. (SLOW!)"""
        for t in TYPE_MAP[1:]:
            if t == Aggregate:
                continue
            for m in CHUNK_MAPPER_MAP[1:]:
                vname = "%s_%s" % (t,m)
                var = self.db.add_var(vname, t, self.step, m)
                name = m.name(self.ts)
                begin = m.begin(name)
                end = m.end(name)
                size = m.size(name, t.size({}), self.step)
                
                r = range(0, (size/t.size({})) * self.step, self.step)

                # write a full chunk of data
                for i in r:
                    v = t(begin+i, ROW_VALID, i)
                    var.insert(v)

                # see if the file is the right size
                f = os.path.join(TESTDB, vname, name)
                if os.stat(f).st_size != size:
                    raise "chunk is wrong size:"

                # read each value to check that the data got written correctly
                for i in r:
                    v = var.get(begin+i)
                    if v.value != i:
                        raise "data bad at %s", str(i) + " " + str(begin+i) + " " + str(v)

                if m.name == m.name(begin-1):
                    raise "lower chunk boundary is incorrect"

                if m.name == m.name(end+1):
                    raise "upper chunk boundary is incorrect"

                for i in (begin, end):
                    var.insert( t(i, ROW_VALID, i) )
                    if var.get(i).value != i:
                        raise "incorrect value at " + str(i)

                    f = os.path.join(TESTDB, vname, m.name(i))
                    if os.stat(f).st_size != m.size(m.name(i), t.size({}),
                            self.step):
                        raise "chunk is wrong size at: " + str(i)

class TestNoAggregates(TSDBVarTestCase):
    def testNoAggregates(self):
        assert self.v.list_aggregates() == []

class AggregatorSmokeTest(TSDBTestCase):
    def setUp(self):
        TSDBTestCase.setUp(self)
        self.var = self.db.add_var("bar", Counter32, 60*60, YYYYMMDDChunkMapper)
        for i in range(24):
            self.var.insert(Counter32(i*60*60, ROW_VALID, i*5*60*60))

        self.var.add_aggregate("+1h", 60*60, YYYYMMDDChunkMapper, ['average','delta'])
        self.var.add_aggregate("+6h", 6*60*60, YYYYMMChunkMapper,
                ['average','delta','min','max'])

        self.var.update_all_aggregates()

    def testFileStructure(self):
        """Do the aggregates get put in the right place?"""
        path = os.path.join(self.var.path, "TSDBAggregates")
        self.assertTrue(os.path.isdir(path))
        self.assertTrue(os.path.isdir(os.path.join(path, "+1h")))
        self.assertTrue(os.path.isfile(os.path.join(path, "+1h", "TSDBVar")))
        self.assertTrue(os.path.isdir(os.path.join(path, "+6h")))
        self.assertTrue(os.path.isfile(os.path.join(path, "+6h", "TSDBVar")))

    def testListAggregates(self):
        self.assertEqual(["+1h", "+6h"], self.var.list_aggregates())

    def testAggregatorAttributes(self):
        """Make sure the size and pack attributes are created correctly."""

        def check_agg(agg_name, aggs, size, pack_format):
            agg = self.var.get_aggregate(agg_name)
            val = agg.get(0)

            print size, val.size({'AGGREGATES': aggs}), val
            self.assertTrue(size == val.size({'AGGREGATES': aggs}))
            self.assertTrue(pack_format == val.get_pack_format({'AGGREGATES': aggs}))

            for af in aggs:
                self.assertTrue(hasattr(val, af))

        check_agg("+1h", ["average", "delta"], 8 + 2*8, "!LLdd")
        check_agg("+6h", ["average", "delta", "min", "max"], 8 + 4*8, "!LLdddd")

    def testAggregatorAverage(self):
        """Test the average aggregator.

        The counter is incremented by 5*60*60 at each each step, so the
        average should be 5 at each step."""

        def test_constant_rate(agg_name, lo, hi, step, rate):
            agg = self.var.get_aggregate(agg_name)
            print agg_name, agg, lo, hi, step, rate
            for i in range(lo, hi):
                print i * step, rate, agg.get(i * step).average
                assert rate == (agg.get(i * step).average)

        test_constant_rate("+1h", 1, 23, 3600, 5.0)
        test_constant_rate("+6h", 1, 3, 6*3600, 5.)

        # add test_zero_rate

    def testAggregatorDelta(self):
        """Test the delta aggregator."""

        def test_constant_delta(agg_name, lo, hi, step, delta):
            agg = self.var.get_aggregate(agg_name)
            print agg_name, lo, hi, step, delta
            for i in range(lo, hi):
                print delta, agg.get(i * step).delta
                self.assertEqual(delta, agg.get(i * step).delta)

        test_constant_delta("+1h", 1, 23, 3600, 5*3600)
        test_constant_delta("+6h", 1, 3, 6*3600, 5*6*3600)

    def tearDown(self):
        os.system("rm -rf %s.agg" % (TESTDB))
        print "hithere"
        os.system("mv %s %s.agg" % (TESTDB, TESTDB))

class TestTSDBRows(unittest.TestCase):
    def testSizes(self):
        """Make sure we are computing the size of rows correctly."""
        assert TSDBRow.size({}) == 8
        assert Counter32.size({}) == 12
        assert Counter64.size({}) == 16
        assert TimeTicks.size({}) == 12
        assert Gauge32.size({}) == 12

        assert Aggregate.size(
                {'AGGREGATES': ['average']}) == 16
        assert Aggregate.size(
                {'AGGREGATES': ['average','delta']}) == 24
        assert Aggregate.size(
                {'AGGREGATES': ['average','delta','min']}) == 32
        assert Aggregate.size(
                {'AGGREGATES': ['average','delta','min','max']}) == 40

        assert Aggregate.get_pack_format(
                {'AGGREGATES': ['average']}) == "!LLd"
        assert Aggregate.get_pack_format(
                {'AGGREGATES': ['average','delta']}) == "!LLdd"
        assert Aggregate.get_pack_format(
                {'AGGREGATES': ['average','delta','min']}) == "!LLddd"
        assert Aggregate.get_pack_format(
                {'AGGREGATES': ['average','delta','min','max']}) == "!LLdddd"

    def testRows(self):
        """Test that we get back what we put in."""
        for r in (Counter32, Counter64, TimeTicks, Gauge32):
            row = r(1,1,1)
            assert row == r.unpack(row.pack({}), {})

    def testAggregate(self):
        """Test that we get back what we put in for Aggregates."""
        m = {'AGGREGATES': ['average','delta','min','max']}
        agg0 = Aggregate(1, 1, average=1, delta=2, min=3, max=4)
        print agg0
        agg1 = Aggregate.unpack(agg0.pack(m), m)
        print agg1
        assert agg0 == agg1

class TestNonDecreasing(TSDBTestCase):
    def testReset(self):
        for rtype in (Counter32, Counter64):
            varname = "test_%s" % (rtype.__name__)
            t = self.db.add_var(varname, rtype, 60, YYYYMMDDChunkMapper)
            t.insert(rtype(1, ROW_VALID, 100))
            t.insert(rtype(60, ROW_VALID, 60))
            t.flush()

            u = self.db.add_var(varname + "_uptime", TimeTicks, 60,
                    YYYYMMDDChunkMapper)
            u.insert(TimeTicks(1, ROW_VALID, 100))
            u.insert(TimeTicks(60, ROW_VALID, 1))

            t.add_aggregate("+60s", 60, YYYYMMDDChunkMapper, ['average','delta'])
            t.update_all_aggregates(uptime_var=u)
            a = t.get_aggregate('+60s')
            print a.get(1).delta

            assert a.get(1).delta == 60

    def testRollover(self):
        print "ROLLIN"
        for rtype, maxval in (
                (Counter32, 4294963596),
                (Counter64, 18446744073709547916)):
            varname = "test_%s" % (rtype.__name__)
            t = self.db.add_var(varname, rtype, 60, YYYYMMDDChunkMapper)
            t.insert(rtype(0, ROW_VALID, maxval))
            t.insert(rtype(60, ROW_VALID, 37))
            t.flush()
            print t

            u = self.db.add_var(varname + "_uptime", TimeTicks, 60,
                    YYYYMMDDChunkMapper)
            u.insert(TimeTicks(0, ROW_VALID, 100))
            u.insert(TimeTicks(60, ROW_VALID, 6100))
            print u

            t.add_aggregate("+60s", 60, YYYYMMDDChunkMapper, ['average','delta'])
            t.update_all_aggregates(uptime_var=u)
            a = t.get_aggregate('+60s')
            print a.get(1).delta
            assert a.get(1).delta == 3737

    def tearDown(self):
        os.system("rm -rf %s.nd" % (TESTDB))
        os.system("mv %s %s.nd" % (TESTDB, TESTDB))
        
if __name__ == "__main__":
    print "these tests create large files, it may take a bit for them to run"
    unittest.main()
