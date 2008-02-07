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
            if not hasattr(t, 'size'):
                continue
            for m in CHUNK_MAPPER_MAP[1:]:
                vname = "%s_%s" % (t,m)
                var = self.db.add_var(vname, t, self.step, m)
                name = m.name(self.ts)
                begin = m.begin(name)
                end = m.end(name)
                size = m.size(name, t.size, self.step)
                
                r = range(0, (size/t.size) * self.step, self.step)

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
                    if os.stat(f).st_size != m.size(m.name(i), t.size,
                            self.step):
                        raise "chunk is wrong size at: " + str(i)

class AggregatorSmokeTest(TSDBTestCase):
    def setUp(self):
        TSDBTestCase.setUp(self)
        self.var = self.db.add_var("bar", Counter32, 60*60, YYYYMMDDChunkMapper)
        for i in range(23):
            self.var.insert(Counter32(i*60*60, ROW_VALID, i*5*60*60))

        self.var.add_aggregate("+1h", 60*60, YYYYMMDDChunkMapper, ['average','delta'])
        self.var.add_aggregate("+6h", 6*60*60, YYYYMMChunkMapper,
                ['average','delta','min','max'])

        self.var.update_all_aggregates()

    def testFileStructure(self):
        """Do the aggregates get put in the right place?"""
        path = os.path.join(self.var.path, "TSDBAggregates")
        print path
        self.assertTrue(os.path.isdir(path))
        self.assertTrue(os.path.isdir(os.path.join(path, "+1h")))
        self.assertTrue(os.path.isfile(os.path.join(path, "+1h", "TSDBVar")))
        self.assertTrue(os.path.isdir(os.path.join(path, "+6h")))
        self.assertTrue(os.path.isfile(os.path.join(path, "+6h", "TSDBVar")))

    def testAggregatorAttributes(self):
        """Make sure the size and pack attributes are created correctly."""

        def check_agg(agg_name, aggs, size, pack_format):
            agg = self.var.get_aggregate(agg_name)
            val = agg.get(0)

            self.assertTrue(size == val.size)
            self.assertTrue(pack_format == val.pack_format)

            for af in aggs:
                self.assertTrue(hasattr(val, af))

        check_agg("+1h", ["average", "delta"], 8 + 2*8, "!LLgg")
        check_agg("+6h", ["average", "delta", "min", "max"], 8 + 4*8, "!LLgggg")

    def testAggregatorAverage(self):
        """Test the average aggregator.

        The counter is incremented by 5*60*60 at each each step, so the
        average should be 5 at each step."""

        def test_constant_rate(agg_name, lo, hi, step, rate):
            print agg_name, lo, hi, step, rate
            agg = self.var.get_aggregate(agg_name)
            for i in range(lo, hi):
                self.assertEqual(rate, agg.get(i*step).average)

        test_constant_rate("+1h", 1, 24, 3600, 5)
        test_constant_rate("+6h", 1, 4, 6*3600, 5)

    def testAggregatorDelta(self):
        """Test the delta aggregator."""

        def test_constant_delta(agg_name, lo, hi, step, delta):
            print agg_name, lo, hi, step, delta
            agg = self.var.get_aggregate(agg_name)
            for i in range(lo, hi):
                self.assertEqual(delta, agg.get(i*step))

        test_constant_delta("+1h", 1, 24, 3600, 5*3600)
        test_constant_delta("+6h", 1, 4, 6*3600, 6*3600)

    def tearDown(self):
        os.system("rm -rf %s.agg" % (TESTDB))
        os.system("mv %s %s.agg" % (TESTDB, TESTDB))

if __name__ == "__main__":
    print "these tests create large files, it may take a bit for them to run"
    unittest.main()
