"""
TSDB -- Time Series Database

TSDB is a database optimized for storing time series data.  Data is indexed by
timestamp so that extracting a range of data requires a minimal number of disk
movements.  TSDB breaks data into chunks which hold a configurable range of
data points.  The full chunk is allocated on the first write in order to put
consecutive data points in spatial locality on the disk.  (This of course is
dependant on the underlying filesystem.)

TSDB was designed to never discard data through summarization.  This means
that unless the system implements a policy to expire old data it will remain
around ad infinitum.  Disk is cheap and plentiful, therefore why would you
want to lose data?

Some general comments:

 * All timestamps are seconds since the epoch in UTC
 * All data is stored in network byte order

Author: Jon M. Dugan <jdugan@es.net>

Copyright (c) 2007-2008, The Regents of the University of California, through
Lawrence Berkeley National Laboratory (subject to receipt of any required
approvals from the U.S. Dept. of Energy).  All rights reserved. 

>>> db = TSDB.create("tsdb_example")
>>> set = db.add_set("example_set")
>>> var = set.add_var("example_var", Counter32, 60, YYYYMMDDChunkMapper)
"""

import mmap
import os
import os.path
import warnings
import time

from tsdb.base import TSDB, TSDBVar, TSDBSet
from tsdb.row import ROW_VALID
from tsdb.error import *
from tsdb.chunk_mapper import YYYYMMChunkMapper, YYYYMMDDChunkMapper

__version__ = ""

# XXX this might be evil
os.environ['TZ'] = "UTC"
time.tzset()

def _doctest_cleanup():
    """
    This is here to clean up the example created by the doc tests:
    >>> import os
    >>> os.system("rm -rf tsdb_example")
    0
    """
