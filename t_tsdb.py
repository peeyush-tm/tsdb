import unittest
import os

from tsdb import *

class CreateTSDB(unittest.TestCase):
    def testCreate(self):
        """can we create a db?"""
        try:
            TSDB.create("tsdb_test")
        except:
            self.fail("unable to create db")

    def testRecreate(self):
        """does trying to create the same db again fail?"""
        TSDB.create("tsdb_test")
        self.assertRaises(TSDBAlreadyExistsError, TSDB.create, "tsdb_test")

    def tearDown(self):
        os.system("rm -rf tsdb_test")

class CreateTSDBSet(unittest.TestCase):
    def setUp(self):
        self.db = TSDB.create("tsdb_test")

    def tearDown(self):
        os.system("rm -rf tsdb_test")

    def testCreate(self):
        """can we create a TSDBSet?"""
        try:
            self.db.add_set("foo")
        except:
            self.fail()

    def testRecreate(self):
        self.db.add_set("foo")
        self.assertRaises(TSDBNameInUseError, self.db.add_set, "foo")

class CreateTSDBSet(unittest.TestCase):
    def setUp(self):
        self.db = TSDB.create("tsdb_test")
        self.set = self.db.add_set("foo")

    def tearDown(self):
        os.system("rm -rf tsdb_test")

    def testCreate(self):
        """can we create a TSDBVar?"""
        try:
            self.set.add_var("bar", Counter32, 60, YYYYMMDDChunkMapper)
        except:
            self.fail()

    def testRecreate(self):
        self.set.add_var("bar", Counter32, 60, YYYYMMDDChunkMapper)
        self.assertRaises(TSDBNameInUseError, self.set.add_var, "bar", Counter32, 60, YYYYMMDDChunkMapper)

class TestData(unittest.TestCase):
    ts = 1184863723
    step = 60

    def setUp(self):
        self.db = TSDB.create("tsdb_test")
        
    def tearDown(self):
        os.system("rm -rf tsdb_test")

    def testData(self):
        for t in TYPE_MAP[1:]:
            for m in CHUNK_MAPPER_MAP[1:]:
                vname = "%s_%s" % (t,m)
                var = self.db.add_var(vname, t, self.step, m)
                name = m.name(self.ts)
                begin = m.begin(name)
                size = m.size(name, t.size, self.step)
                
                r = range(0, (size/t.size) * self.step, self.step)

                # write a full chunk of data
                for i in r:
                    v = t(begin+i, ROW_VALID, i)
                    var.insert(v)

                # see if the file is the right size
                f = os.path.join("tsdb_test", vname, name)
                if os.stat(f).st_size != size:
                    raise "chunk is wrong size:"

                # read each value to check that the data got written correctly
                for i in r:
                    v = var.select(begin+i)
                    if v.value != i:
                        raise "data bad at %s", str(i) + " " + str(begin+i) + " " + str(v)

                low = begin-1
                if m.name == m.name(low):
                    raise "lower chunk boundary is incorrect"

                high = begin + ((size/t.size)*self.step) + 1
                if m.name == m.name(high):
                    raise "upper chunk boundary is incorrect"

                for i in (low,high):
                    var.insert( t(i, ROW_VALID, i) )
                    if var.select(i).value != i:
                        raise "incorrect value at " + str(i)

                    f = os.path.join("tsdb_test", vname, m.name(i))
                    if os.stat(f).st_size != m.size(m.name(i), t.size,
                            self.step):
                        raise "chunk is wrong size at: " + str(i)


        
if __name__ == "__main__":
    print "these tests create large files, it may take a bit for them to run"
    unittest.main()
