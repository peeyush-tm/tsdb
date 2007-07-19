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

if __name__ == "__main__":
    unittest.main()
