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

import struct
import mmap
import os
import os.path
import time
import calendar
import warnings
import re
import itertools

from math import floor, ceil

from fpconst import isNaN

__version__ = ""

os.environ['TZ'] = "UTC"
time.tzset()

class TSDBError(Exception):
    """Base class for TSDB Specific Exceptions"""
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.__str__()

class TSDBAlreadyExistsError(TSDBError):
    """The TSDB creation request would overwrite an exisiting TSDB."""
    pass

class TSDBSetDoesNotExistError(TSDBError):
    """The requested TSDBSet does not exist."""
    pass

class TSDBVarDoesNotExistError(TSDBError):
    """The requested TSDBVar does not exist."""
    pass

class TSDBAggregateDoesNotExistError(TSDBError):
    """The requested TSDBVar does not exist."""
    pass

class TSDBVarChunkDoesNotExistError(TSDBError):
    """The requested TSDBVarChunk does not exist."""
    pass

class TSDBVarRangeError(TSDBError):
    """The requested datapoint has a timestamp out side the range of available
    data."""
    pass

class TSDBVarEmpty(TSDBError):
    """There are no data chunks in the variable."""
    pass

class TSDBVarIsNotAggregate(TSDBError):
    """This isn't an aggregate."""
    pass

class TSDBNameInUseError(TSDBError):
    """The name requested is already in use."""
    pass

class InvalidInterval(TSDBError):
    """The interval specification did not parse."""
    pass

class TSDBRow(object):
    """A TSDBRow represents a datapoint inside a TSDBVar.

    All values are stored in network byte order (big endian).

    A TSDBRow may have configurable fields (eg. Aggregate).  TSDBRows are
    fixed in size for a given TSDBVar, each subclass defines the size."""

    type_id = 0
    version = 1
    pack_format = "!LL"
    can_rollover = False

    def __init__(self, timestamp, flags, value):
        if self.type_id == 0:
            raise NotImplementedError("TSDBRow is an abstract type") 
        self.timestamp = int(timestamp)
        self.flags = flags
        self.value = value

        if type(self.value) == str:
            self.value = self._from_str(value)

    def _from_str(self, str):
        """Convert string to a row."""
        raise NotImplementedError("not implemented in TSDBRow")

    def pack(self, metadata):
        """Pack the row into a binary string."""
        return struct.pack(self.pack_format, self.timestamp, self.flags,
                self.value)

    @classmethod
    def size(klass, metadata):
        return struct.calcsize(klass.pack_format)

    @classmethod
    def unpack(klass, s, metadata):
        """Unpack binary string into an instance."""
        return klass(*struct.unpack(klass.pack_format, s))

    def __str__(self):
        return "%s: [%d/%#x: %d]" % (self.__class__.__name__, self.timestamp,
                self.flags, self.value)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, TSDBRow):
            return self.timestamp == other.timestamp \
                    and self.flags == other.flags \
                    and self.value == other.value \
                    and self.type_id == other.type_id
        else:
            return NotImplemented

    @classmethod
    def rollover(klass, value):
        raise NotImplementedError("not implemented in TSDBRow")

    def invalidate(self):
        self.flags &= ~ROW_VALID

class Counter32(TSDBRow):
    """Represent a SNMP Counter32 variable.

    >>> x = Counter32(1,1,1) 
    >>> y = Counter32(1,1,1)
    >>> x == y
    True
    >>> z = Counter32(1,1,2)
    >>> x == z
    False
    """

    type_id = 1
    version = 1
    pack_format = TSDBRow.pack_format + "L"
    can_rollover = True
    
    def _from_str(self, str):
        return int(str)

    @classmethod
    def rollover(klass, value):
        return value + 2**32

class Counter64(TSDBRow):
    """Represent a SNMP Counter64 variable.

    >>> x = Counter64(1,1,1) 
    >>> y = Counter64(1,1,1)
    >>> x == y
    True
    >>> z = Counter64(1,1,2)
    >>> x == z
    False
    """
    type_id = 2
    version = 1
    pack_format = TSDBRow.pack_format + "Q"
    can_rollover = True

    def _from_str(self, str):
        return long(str)

    @classmethod
    def rollover(klass, value):
        return value + 2**64

class Gauge32(TSDBRow):
    """Represent a SNMP Gauge32 variable.

    >>> x = Gauge32(1,1,1) 
    >>> y = Gauge32(1,1,1)
    >>> x == y
    True
    >>> z = Gauge32(1,1,2)
    >>> x == z
    False
    """
    type_id = 3
    version = 1
    pack_format = TSDBRow.pack_format + "L"

    def _from_str(self, str):
        return int(str)

class TimeTicks(TSDBRow):
    """Represent a SNMP TimeTicks variable.

    >>> x = TimeTicks(1,1,1) 
    >>> y = TimeTicks(1,1,1)
    >>> x == y
    True
    >>> z = TimeTicks(1,1,2)
    >>> x == z
    False
    """
    type_id = 4
    version = 1
    pack_format = TSDBRow.pack_format + "L"

    def _from_str(self, str):
        return int(str)

class Aggregate(TSDBRow):
    """Represent an aggregate value, such as an average.
    
    An Aggregate computes values based on raw data or a finer grained
    aggregate.  An Aggregate is not an instantaneous value, but is computed
    for some time interval.  This interval is stored as STEP in the metadata.

    An Aggregate can store one or more of these aggregates:

        average - arithmetic mean
        min     - minimum value
        max     - maximum value
        delta   - total change for time interval

    This list of which aggregates should be stored is kept in the AGGREGATES
    metadata entry for each Aggregate.  The Aggregate TSDBRow has a variable
    size but all Aggregate rows in a given TSDBVar are the same size.

    Only the types listed in the AGGREGATES metadata field will be stored.
    Note that an Aggregate my have values for more aggregates than it stores.
    What is stored is determined by the metadata passed in to the pack
    method."""

    aggregate_order = ('average', 'delta', 'min', 'max')
    type_id = 5
    version = 1

    def __init__(self, timestamp, flags, **kwargs):
        self.timestamp = int(timestamp)
        self.flags = flags
        
        for agg in self.aggregate_order:
            if kwargs.has_key(agg):
                if kwargs[agg] is None:
                    val = float('NaN')
                else:
                    val = float(kwargs[agg])

                setattr(self, agg, val)

    def pack(self, metadata):
        l = []
        for agg in self.aggregate_order:
            if agg in metadata['AGGREGATES']:
                l.append(getattr(self, agg))

        return struct.pack(Aggregate.get_pack_format(metadata), self.timestamp, self.flags, *l)

    @classmethod
    def get_pack_format(klass, metadata):
        pack_format = TSDBRow.pack_format
        for agg in klass.aggregate_order:
            if agg in metadata['AGGREGATES']:
                pack_format += 'd'
        return pack_format

    @classmethod
    def size(klass, metadata):
        return struct.calcsize(klass.get_pack_format(metadata))

    @classmethod
    def unpack(klass, s, metadata):
        args = struct.unpack(klass.get_pack_format(metadata), s)
        kwargs = {}
        i = 0
        for agg in klass.aggregate_order:
            if agg in metadata['AGGREGATES']:
                kwargs[agg] = args[2+i]
                i += 1

        return klass(*args[:2], **kwargs)

    def __str__(self):
        l = []
        for agg in self.aggregate_order:
            if hasattr(self, agg):
                l.append("%s=%g" % (agg, getattr(self, agg)))

        return "%s: [%d/%#x: %s]" % (self.__class__.__name__, self.timestamp,
                self.flags, " ".join(l))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, TSDBRow):
            if self.timestamp == other.timestamp \
               and self.flags == other.flags \
               and self.type_id == other.type_id:
                for agg in self.aggregate_order:
                    if not (hasattr(self, agg) and hasattr(other, agg) \
                       and getattr(self, agg) == getattr(other, agg)):
                        return False
                return True
            else:
                return False
        else:
            return NotImplemented

"""
TSDB reserves bits 0-7 (the low 8 bits) of the flag field for globally defined flags. 
Bits 8-15 are reserved for Row specific flags.  Bits 16-31 are for application
specific uses.
"""

ROW_VALID   = 0x0001  # does this row have valid data?
ROW_WRAP    = 0x0002  # was there a wrap between this entry and the previous
ROW_UNWRAP  = 0x0004  # the wrap for this entry was corrected

TYPE_MAP = [TSDBRow, Counter32, Counter64, Gauge32, TimeTicks, Aggregate]


# XXX end times are to the nearest second, we're storing timestamps as ints so
# it's ok, but this doesn't work right if we compare with float times.

class ChunkMapper(object):
    """A ChunkMapper maps data in a TSDBVar into a chunk (disk file).

    ChunkMapper is the base class for all ChunkMappers.

    A ChunkMapper takes a timestamp in seconds since the epoch and returns a
    string which is the name of the chunk containing that timestamp."""

    chunk_mapper_id = 0
    def __init__(self):
        if self.chunk_mapper_id == 0:
            raise NotImplementedError("ChunkMapper is an abstract class")

    def name(self, timestamp):
        """Return the name of the chunk."""
        raise NotImplementedError("name")

    def begin(self, name):
        """Return the minimum timestamp storable in this chunk."""
        raise NotImplementedError("begin")

    def end(self, name):
        """Return the maximum timestamp storable in this chunk."""
        raise NotImplementedError("end")

    def size(self, name, row_size, step):
        """Return the size of the chunk file on disk."""
        raise NotImplementedError("size")

class YYYYMMChunkMapper(ChunkMapper):
    """
    Breaks variables into monthly chunks.

    >>> m = YYYYMMChunkMapper()
    >>> n = m.name(1184726536)
    >>> n
    '200707'
    >>> m.begin(n)
    1183248000
    >>> m.size(n, 1, 60)
    44640
    """

    chunk_mapper_id = 1

    @classmethod
    def name(klass, timestamp):
        return  "%04d%02d" % time.gmtime(timestamp)[:2]

    @classmethod
    def begin(klass, name):
        return calendar.timegm((int(name[:4]), int(name[-2:]), 1, 0, 0, 0))

    @classmethod
    def end(klass, name):
        Y=int(name[:4])
        M=int(name[-2:])
        return calendar.timegm((Y, M, calendar.monthrange(Y, M)[-1], 23, 59, 59))

    @classmethod
    def size(klass, name, row_size, step):
        begin = klass.begin(name)
        (begin_year, begin_month) = (int(name[:4]), int(name[-2:]))
        if begin_month == 12:
            end_year = begin_year + 1
            end_month = 1
        else:
            end_year = begin_year
            end_month = begin_month + 1

        end = calendar.timegm((end_year, end_month, 1, 0, 0, 0))

        return ((end - begin)/step)*row_size

class YYYYMMDDChunkMapper(ChunkMapper):
    """
    Breaks variables into daily chunks.

    >>> m = YYYYMMDDChunkMapper()
    >>> n = m.name(1184726536)
    >>> n
    '20070718'
    >>> m.begin(n)
    1184716800
    >>> m.size(n, 1, 60)
    1440
    """
    chunk_mapper_id = 2

    @classmethod
    def name(klass, timestamp):
        return  "%04d%02d%02d" % time.gmtime(timestamp)[:3]

    @classmethod
    def begin(klass, name):
        return calendar.timegm(
                (int(name[:4]), int(name[4:6]), int(name[6:]), 0, 0, 0))
    
    @classmethod
    def end(klass, name):
        return calendar.timegm(
                (int(name[:4]), int(name[4:6]), int(name[6:]), 23, 59, 59))

    @classmethod
    def size(klass, name, row_size, step):
        begin = klass.begin(name)
        (begin_year, begin_month, begin_day) = \
                (int(name[:4]), int(name[4:6]), int(name[6:]))
        (wdays, mdays) = calendar.monthrange(begin_year, begin_month)


        if begin_day == mdays:
            if begin_month == 12:
                end_year = begin_year + 1
                end_month = 1
            else:
                end_year = begin_year
                end_month = begin_month + 1
            end_day = 1
        else:
            end_year = begin_year
            end_month = begin_month
            end_day = begin_day + 1

        end = calendar.timegm((end_year, end_month, end_day, 0, 0, 0))

        return ((end - begin)/step)*row_size

class EpochWeekMapper(ChunkMapper):
    """
    maps into chunks that are 7*24*60*60 seconds long, starting at the unix
    epoch.

    This is an experimental mapper attempting to improve performance.

    >>> m = EpochWeekMapper()
    >>> n = m.name(1184726536)
    >>> n
    '1958'
    >>> m.begin(n)
    1184198400
    >>> m.size(n, 1, 60)
    10080
    """
    chunk_mapper_id = 3
    weeksecs = 604800

    @classmethod
    def name(klass, timestamp):
        return str(timestamp / klass.weeksecs)

    @classmethod
    def begin(klass, name):
        return int(name) * klass.weeksecs

    @classmethod
    def end(klass, name):
        return (int(name)+1) * klass.weeksecs - 1

    @classmethod
    def size(klass, name, row_size, step):
        return (klass.weeksecs/step) * row_size


CHUNK_MAPPER_MAP = [ChunkMapper, YYYYMMChunkMapper, YYYYMMDDChunkMapper,
        EpochWeekMapper]
  
def write_dict(path, d):
    """Write a dictionary in NAME: VALUE format."""
    f = open(path, "w")

    for key in d:
        f.write(key + ": " + str(d[key]) + "\n")

    f.close()

class TSDBBase(object):
    """TSDBBase is a base class for other TSDB containers.

    It is abstract and should not be instantiated."""

    tag = None
    metadata_map = None

    def __init__(self, metadata=None):
        if self.tag is None:
            raise NotImplementedError("TSDBBase is abstract.")

        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata
        self.path = None
        self.vars = {}
        self.sets = {}

    def __str__(self):
        return "%s [%s]" % (self.tag, self.path)

    def load_metadata(self):
        """Load metadata for this container.

        Metadata is stored in the file specified by the tag class attribute.

        Data is stored in the format:

        NAME: VALUE

        With one name/value pair per line.  Lists are stored as the str()
        representation of the actual list."""

        f = open(os.path.join(self.path, self.tag), "r")

        for line in f:
            line = line.strip()
            if line.startswith("#"):
                continue
            (var, val) = line.split(':', 1)
            val = val.strip()
            # XXX probably want to revist this
            if self.metadata_map.has_key(var):
                if self.metadata_map[var] == list:
                    val = eval(val)
                else:
                    val = self.metadata_map[var](val)
            self.metadata[var] = val

        f.close()

    def save_metadata(self):
        """Save metadata for this container."""
        write_dict(os.path.join(self.path, self.tag), self.metadata)

    def list_sets(self):
        """List TSDBSets in this container."""
        return filter( \
                lambda x: TSDBSet.is_tsdb_set(os.path.join(self.path, x)),
                os.listdir(self.path))

    def get_set(self, name):
        """Get named TSDBSet."""
        if not self.sets.has_key(name):
            self.sets[name] = TSDBSet(self, os.path.join(self.path, name))

        return self.sets[name]

    def add_set(self, name):
        """Create a new TSDBSet in this container."""
        prefix = self.path
        tsdb_set = self
        steps = name.split('/')
        for step in steps[:-1]:
            try:
                tsdb_set = tsdb_set.get_set(step)
            except TSDBSetDoesNotExistError:
                TSDBSet.create(prefix, step)
                tsdb_set = tsdb_set.get_set(step)

            prefix = os.path.join(prefix, step)

        TSDBSet.create(prefix, steps[-1])
        tsdb_set = tsdb_set.get_set(steps[-1])

        return tsdb_set

    def list_vars(self):
        """List TSDBVars in this container."""
        return filter(lambda x: \
                TSDBVar.is_tsdb_var(os.path.join(self.path, x)),
                os.listdir(self.path))

    def get_var(self, name, **kwargs):
        """Get named TSDBVar."""
        if not self.vars.has_key(name):
            self.vars[name] = \
                    TSDBVar(self, os.path.join(self.path, name), **kwargs) 
        return self.vars[name]

    def add_var(self, name, type, step, chunk_mapper, metadata=None):
        prefix = os.path.dirname(name)
        """Create a new TSDBVar in this container."""
        if prefix != '':
            try:
                self.get_set(prefix)
            except TSDBSetDoesNotExistError:
                self.add_set(prefix)

        TSDBVar.create(self.path, name, type, step, chunk_mapper, metadata)
        return self.get_var(name)

    def list_aggregates(self):
        """Sorted list of existing aggregates."""

        if not TSDBSet.is_tsdb_set(os.path.join(self.path, "TSDBAggregates")):
            return []

        def is_aggregate(x):
            return TSDBVar.is_tsdb_var(
                    os.path.join(self.path, "TSDBAggregates", x))

        aggs = filter(is_aggregate,
                os.listdir(os.path.join(self.path, "TSDBAggregates")))

        weighted = [ (calculate_interval(x), x) for x in aggs ]
        weighted.sort()
        return [ x[1] for x in weighted ]

    def get_aggregate(self, name):
        """Get an existing aggregate."""
        try:
           set = self.get_set("TSDBAggregates")
        except TSDBSetDoesNotExistError:
            raise TSDBAggregateDoesNotExistError(name)

        name = str(calculate_interval(name))
        try:
            agg = set.get_var(name)
        except TSDBVarDoesNotExistError:
            raise TSDBAggregateDoesNotExistError(name)

        return agg

    def add_aggregate(self, name, step, chunk_mapper, aggregates,
            metadata=None):
        """Add an aggregate at the current level.
        
        aggregates is a list of strings containing the names of the aggregates
        to compute.
        """
        # XXX should add error checking to aggregates?
        if metadata is None:
            metadata = {}

        metadata['AGGREGATES'] = aggregates
        metadata['LAST_UPDATE'] = 0

        if not metadata.has_key('VALID_RATIO'):
            metadata['VALID_RATIO'] = 0.5

        try:
            aggset = self.get_set("TSDBAggregates")
        except:
            aggset = self.add_set("TSDBAggregates")

        name = str(calculate_interval(name))
        return aggset.add_var(name, Aggregate, step, chunk_mapper, metadata)

    @classmethod
    def is_tag(klass, path):
        """Is the current container a TSDB container of type tag?"""
        if os.path.isdir(path) and \
            os.path.isfile(os.path.join(path,klass.tag)):
            return True
        else:
            return False

class TSDB(TSDBBase):
    """A time series data base (TSDB).

    Each TSDB is made up of collection of sets and variables.  Each set or
    variable may contain any arbitrary collection of sets and variables.

    """

    tag = "TSDB"
    metadata_map = {}

    def __init__(self, path):
        """Load the TSDB at path."""
        TSDBBase.__init__(self)
        self.path = path
        self.load_metadata()

    @classmethod
    def is_tsdb(klass, path):
        """Does path contain a TSDB?"""
        return klass.is_tag(path)

    @classmethod 
    def create(klass, path, metadata=None):
        """Create a new TSDB."""
        if metadata is None:
            metadata = {}

        if os.path.exists(os.path.join(path, "TSDB")):
            raise TSDBAlreadyExistsError("database already exists")

        metadata["CREATION_TIME"] = time.time()

        if not os.path.exists(path):
            os.mkdir(path)

        write_dict(os.path.join(path, klass.tag), metadata)

        return klass(path)

class TSDBSet(TSDBBase):
    """A TSDBSet is used to organize TSDBVars into groups.

    A TSDBSet has metadata but no actual data, it is a container for
    TSDBVars."""

    tag = "TSDBSet"
    metadata_map = {}
    def __init__(self, parent, path, metadata=None):
        """Load the TSDBSet at path."""
        TSDBBase.__init__(self)
        self.path = path
        self.parent = parent

        if not os.path.exists(path) or not self.is_tsdb_set(path):
            raise TSDBSetDoesNotExistError("TSDBSet does not exist " + path)

        self.load_metadata()

    @classmethod
    def is_tsdb_set(klass, path):
        """Does path contain a TSDBSet?"""
        return klass.is_tag(path)

    @classmethod
    def create(klass, root, name, metadata=None):
        """Create a new TSDBSet with name name rooted at root."""
        if metadata is None:
            metadata = {}

        path = os.path.join(root, name)
        if os.path.exists(path):
            raise TSDBNameInUseError("%s already exists at %s" % (name, path))

        os.mkdir(path)
        write_dict(os.path.join(path, klass.tag), metadata)

    def lock(self, block=True):
        """Acquire a write lock.

        Note: NOT IMPLEMENTED."""
        warnings.warn("locking not implemented yet")

    def unlock(self):
        """Release a write lock.

        Note: NOT IMPLEMENTED."""
        warnings.warn("locking not implemented yet")

class TSDBVar(TSDBBase):
    """A TSDBVar represent a timeseries.

    A TSDBVar is broken into TSDBVarChunks.

    TSDBVars can be nested arbitrarily, but by convention the only TSDBVars
    inside a TSDBVar are aggregates.  By convetion aggregate sub variables
    will be named +nI, for example 20 minute aggregates would be +20m.  See
    the doc string for expand_interval for acceptable values of I."""

    tag = "TSDBVar"
    metadata_map = {'STEP': int, 'TYPE_ID': int, 'MIN_TIMESTAMP': int,
            'MAX_TIMESTAMP': int, 'VERSION': int, 'CHUNK_MAPPER_ID': int,
            'AGGREGATES': list, 'LAST_UPDATE': int, 'VALID_RATIO': float}

    def __init__(self, parent, path, use_mmap=False, cache_chunks=False,
            metadata=None):
        """Load the TSDBVar at path."""
        TSDBBase.__init__(self)

        if not os.path.exists(path) or not self.is_tsdb_var(path):
            raise TSDBVarDoesNotExistError("TSDBVar does not exist:" + path)

        self.parent = parent
        self.path = path
        self.use_mmap = use_mmap
        self.cache_chunks = cache_chunks

        self.load_metadata()

        self.type = TYPE_MAP[self.metadata['TYPE_ID']]
        self.chunks = {} # memory resident chunks
        self.chunk_mapper = CHUNK_MAPPER_MAP[self.metadata['CHUNK_MAPPER_ID']]

    @classmethod
    def is_tsdb_var(klass, path):
        """Does path contain a TSDBVar?"""
        return klass.is_tag(path)

    @classmethod
    def create(klass, root, name, vartype, step, chunk_mapper, metadata=None):
        """Create a new TSDBVar."""
        if metadata is None:
            metadata = {}

        path = os.path.join(root, name)
        if os.path.exists(path):
            raise TSDBNameInUseError("%s already exists at %s" % (name, path))

        if type(vartype) == str:
            exec("vartype = tsdb.%s" % vartype)
        elif type(vartype) == int:
            vartype = TYPE_MAP[vartype]

        metadata["NAME"] = name
        metadata["TYPE_ID"] = vartype.type_id
        metadata["VERSION"] = vartype.version
        metadata["STEP"] = step
        metadata["CREATION_TIME"] = time.time()
        metadata["CHUNK_MAPPER_ID"] = chunk_mapper.chunk_mapper_id

        os.mkdir(path)

        write_dict(os.path.join(path, klass.tag), metadata)

    def _get_aggregate_ancestor(self, agg_name):
        agg_list = self.list_aggregates()
        idx = agg_list.index(agg_name)
        if idx > 0:
            return self.get_aggregate(agg_list[idx-1])
        else:
            return self

    def update_aggregate(self, name, uptime_var=None):
        """Update the named aggreagate."""
        return Aggregator(self.get_aggregate(name),
                self._get_aggregate_ancestor(name)).update(uptime_var=uptime_var)

    def update_all_aggregates(self, uptime_var=None):
        """Update all aggregates for this TSDBVar."""
        for agg in self.list_aggregates():
            self.update_aggregate(agg, uptime_var=uptime_var)

    def all_chunks(self):
        """Generate a sorted list of all chunks in this TSDBVar."""

        l = filter(\
            lambda x: x != self.tag and \
            not os.path.isdir(os.path.join(self.path,x)), os.listdir(self.path)\
        )
        if not l:
            raise TSDBVarEmpty("no chunks")

        l.sort()
        return l

    def rowsize(self):
        """Returns the size of a row."""
        return self.type.size(self.metadata)

    def _chunk(self, timestamp, create=False):
        """Retrieve the chunk that contains the given timestamp.

        If create is True then create the chunk if it does not exist.  Chunks
        are memoized in the chunks attribute of TSDBVar.

        _chunk is an internal function and should not be called externally.
        """
        name = self.chunk_mapper.name(timestamp)

        if not self.chunks.has_key(name):
            if not self.cache_chunks:
                for chunk in self.chunks.keys():
                    self.chunks[chunk].close()
                    del self.chunks[chunk]

            try:
                self.chunks[name] = \
                        TSDBVarChunk(self, name, use_mmap=self.use_mmap)
            except TSDBVarChunkDoesNotExistError:
                if create:
                    self.chunks[name] = \
                            TSDBVarChunk.create(self, name,
                                                use_mmap=self.use_mmap)
                    self.min_timestamp(recalculate=True)
                    self.max_timestamp(recalculate=True)
                else:
                    raise

        return self.chunks[name]

    def min_timestamp(self, recalculate=False):
        """Finds the minimum possible timestamp for this TSDBVar.
        
        This is the beginning timestamp of the oldest chunk.  It may not be
        the minimum _valid_ timestamp."""
        if recalculate or not self.metadata.has_key('MIN_TIMESTAMP'):
            chunks = self.all_chunks()

            self.metadata['MIN_TIMESTAMP'] = self.chunk_mapper.begin(chunks[0])
            self.save_metadata() # XXX good idea?

        return self.metadata['MIN_TIMESTAMP']

    def max_timestamp(self, recalculate=False):
        """Finds the maximum possible timestamp for this TSDBVar.
        
        This is the ending timestamp of the newest chunk. It may not be the
        maximum _valid_ timestamp."""
        if recalculate or not self.metadata.has_key('MAX_TIMESTAMP'):
            chunks = self.all_chunks()

            self.metadata['MAX_TIMESTAMP'] = self.chunk_mapper.end(chunks[-1])
            self.save_metadata() # XXX good idea?

        return self.metadata['MAX_TIMESTAMP']

    def min_valid_timestamp(self):
        """Finds the timestamp of the minimum valid row."""
        # XXX fails if the oldest chunk is all invalid
        chunk = self._chunk(self.min_timestamp())
        ts = self.metadata['MIN_TIMESTAMP']
        while True:
            row = chunk.read_row(ts)
            if row.flags & ROW_VALID:
                return row.timestamp

            ts += self.metadata['STEP']

    def max_valid_timestamp(self):
        """Finds the timestamp of the maximum valid row."""
        chunk = self._chunk(self.max_timestamp())
        ts = self.metadata['MAX_TIMESTAMP']
        while True:
            row = chunk.read_row(ts)
            if row.flags & ROW_VALID:
                return row.timestamp

            ts -= self.metadata['STEP']

    def get(self, timestamp):
        """Get the TSDBRow located at timestamp."""
        timestamp = int(timestamp)
        try:
            if timestamp < self.min_timestamp():
                raise TSDBVarRangeError(
                        "%d is less the the minimum timestamp %d" % (timestamp,
                            self.min_timestamp()))

            if timestamp > self.max_timestamp():
                raise TSDBVarRangeError(
                        "%d is greater the the maximum timestamp %d" % (timestamp,
                            self.max_timestamp()))
        except TSDBVarEmpty:
            raise TSDBVarRangeError(timestamp)

        try:
            chunk = self._chunk(timestamp)
        except TSDBVarChunkDoesNotExistError:
            raise TSDBVarRangeError(timestamp)

        val = chunk.read_row(timestamp)

        if not val.flags & ROW_VALID:
            val.timestamp = timestamp

        return val

    def select(self, begin=None, end=None, flags=None):
        """Select data based on timestamp or flags.

        None is interpreted as "don't care"

        eg:

        select()
          all data for this var, valid or not

        select(begin=10000)
          all data with a timestamp equal to or greater than 10000

        select(end=10000)
          all data with a timestample equal to or less than 10000

        select(begin=10000, end=20000)
          all data in the range of timestamps 10000 to 20000 inclusive

        select(flags=ROW_VALID)
          all valid data """

        if begin is None:
            begin = self.min_timestamp()
        else:
            begin = int(begin)
            if begin < self.min_timestamp():
                begin = self.min_timestamp()

        if end is None:
            end = self.max_timestamp()
        else:
            end = int(end)

        if flags is not None:
            flags = int(flags)

        def select_generator(var, begin, end, flags):
            current = begin

            while current <= end:
                try:
                    row = var.get(current)
                except TSDBVarRangeError:
                    chunks = self.all_chunks()

                    # looking for data beyond the end of recorded data so stop.
                    
                    if current > self.chunk_mapper.end(chunks[-1]):
                        raise StopIteration

                    # if we've found a gap in chunks, fill it in.
                    # this is inefficient for large gaps, but only once.
                    # there should never be large gaps so this is ok

                    chunk = self._chunk(current, create=True)
                    row = var.get(current)

                valid = True
                if flags is not None and row.flags & flags != flags:
                    valid = False
    
                if valid:
                    yield row

                current += var.metadata['STEP']

            raise StopIteration


        return select_generator(self, begin, end, flags)

    def insert(self, data):
        """Insert data.  

        Data should be a subclass of TSDBRow."""
        chunk = self._chunk(data.timestamp, create=True)
        return chunk.write_row(data)

    def flush(self):
        """Flush all the chunks for this TSDBVar to disk."""
        for chunk in self.chunks:
            self.chunks[chunk].flush()

    def close(self):
        """Close this TSDBVar."""
        self.flush()
        for chunk in self.chunks:
            self.chunks[chunk].close()

    def lock(self, block=True):
        """Acquire a write lock.
        
        Note: NOT IMPLEMENTED."""
        warnings.warn("locking not implemented yet")

    def unlock(self):
        """Release a write lock.

        Note: NOT IMPLEMENTED."""
        warnings.warn("locking not implemented yet")

class TSDBVarChunk(object):
    """A TSDBVarChunk is a physical file containing a portion of the data for
    a TSDBVar."""

    def __init__(self, tsdb_var, name, use_mmap=False):
        """Load the specified TSDBVarChunk."""
        path = os.path.join(tsdb_var.path, name)
        if not os.path.exists(path):
            raise TSDBVarChunkDoesNotExistError(path)

        self.tsdb_var = tsdb_var
        self.path = path
        self.name = name
        self.use_mmap = use_mmap

        self.file = open(self.path, "r+")
        self.size = os.path.getsize(self.path)

        if self.use_mmap:
            self.mmap = mmap.mmap(self.file.fileno(), self.size)
            self.io = self.mmap
        else:
            self.io = self.file

        self.begin = tsdb_var.chunk_mapper.begin(os.path.basename(self.path))

    @classmethod
    def create(klass, tsdb_var, name, use_mmap=False):
        """Create the named TSDBVarChunk."""
        print "%s creating %s" % (tsdb_var.path, name)
        path = os.path.join(tsdb_var.path, name)
        f = open(path, "w")
        f.write("\0" * tsdb_var.chunk_mapper.size(os.path.basename(path),
            tsdb_var.rowsize(), tsdb_var.metadata['STEP']))
        f.close()
        return TSDBVarChunk(tsdb_var, name, use_mmap=use_mmap)

    def flush(self):
        """Flush this TSDBVarChunk to disk."""
        return self.io.flush()

    def close(self):
        """Close this TSDBVarChunk."""
        return self.io.close()

    def seek(self, position, whence=0):
        """Seek to the specified position."""
        return self.io.seek(position, whence)

    def tell(self):
        """Get the current position in the chunk."""
        return self.io.tell()

    def write(self, s):
        """Write data at the current position."""
        return self.io.write(s)

    def read(self, n):
        """Read n bytes starting at the current position."""
        return self.io.read(n)

    def _offset(self, timestamp):
        """Calculate the offset chunk for a timestamp.
        
        This offset is relative to the beginning of this TSDBVarChunk."""
        o = ((timestamp - self.begin) / self.tsdb_var.metadata['STEP']) \
                * self.tsdb_var.rowsize()
        return o

    def write_row(self, data):
        """Write a TSDBRow to disk."""
        if self.use_mmap:
            o = self._offset(data.timestamp)
            self.mmap[o:o+self.tsdb_var.rowsize()] = \
                    data.pack(self.tsdb_var.metadata)
        else:
            self.io.seek(self._offset(data.timestamp))
            return self.io.write(data.pack(self.tsdb_var.metadata))

    def read_row(self, timestamp):
        """Read a TSDBRow from disk."""
        if self.use_mmap:
            o = self._offset(timestamp)
            return self.tsdb_var.type.unpack(
                    self.mmap[o:o+self.tsdb_var.rowsize()],
                    self.tsdb_var.metadata)
        else:
            self.io.seek(self._offset(timestamp))
            return self.tsdb_var.type.unpack(
                    self.io.read(self.tsdb_var.rowsize()),
                    self.tsdb_var.metadata)

class Aggregator(object):
    """Calculate Aggregates.
    
    XXX ultimately there should be an aggregator for each class of TSDBVars.
    This one is really targeted at Counters and should become
    CounterAggregator.  It should be possible to generalize some of this
    functionality into a base Aggregator class though."""

    def __init__(self, agg, ancestor, max_rate=10*1000*1000*1000):
        self.agg = agg
        self.ancestor = ancestor
        self.max_rate = max_rate

    def _empty_row(self, var, timestamp):
        aggs = {}
        for a in var.metadata['AGGREGATES']:
            aggs[a] = 0

        return var.type(timestamp, 0, **aggs)

    def _increase_delta(self, var, timestamp, value):
        if var.type != Aggregate:
            raise TSDBVarIsNotAggregate("not an Aggregate")

        try:
            row = var.get(timestamp)
        except (TSDBVarEmpty, TSDBVarRangeError):
            row = self._empty_row(var, timestamp)

        row.delta += value
        row.flags |= ROW_VALID
        var.insert(row)

    def update(self, uptime_var=None):
        if self.ancestor.type == Aggregate:
            self.update_from_aggregate()
        else:
            self.update_from_raw_data(uptime_var=uptime_var)

        self.agg.flush()

    def update_from_raw_data(self, uptime_var=None):
        """Update this aggregate from raw data.

        The first aggregate MUST have the same step as the raw data.  (This
        the only aggregate with a raw data ancestor.)

        Scan all of the new data and bin it in the appropriate place.  At
        either end a bin may have only partial data.  Detect and handle
        rollovers.  We process all data with a timestamp >= begin and
        with ROW_VALID set."""

        step = self.agg.metadata['STEP']
        assert self.ancestor.metadata['STEP'] == step

        last_update = self.agg.metadata['LAST_UPDATE']
        min_ts = self.ancestor.min_valid_timestamp()
        if min_ts > last_update:
            last_update = min_ts
            self.agg.metadata['LAST_UPDATE'] = last_update
       
        prev = self.ancestor.get(last_update)

        # XXX this only works for Counter types right now
        for curr in self.ancestor.select(begin=last_update+step, flags=ROW_VALID):
            delta_t = curr.timestamp - prev.timestamp
            delta_v = curr.value - prev.value

            if self.ancestor.type.can_rollover and delta_v < 0:
                assert uptime_var is not None 
                delta_uptime = uptime_var.get(curr.timestamp).value - \
                        uptime_var.get(prev.timestamp).value

                if delta_uptime < 0:
                    # this is a reset
                    delta_v = curr.value
                else:
                    delta_v = self.ancestor.type.rollover(delta_v)


            assert delta_v >= 0
            #
            # tests for edge cases: rollover, invalid, large gaps in data
            # not sure how to properly invalidate individual rows

            # allocate a portion of this data to a given bin
            prev_slot = (prev.timestamp / step) * step
            prev_frac = int(ceil(delta_v * (step - (prev.timestamp - prev_slot)) /
                float(delta_t)))

            curr_slot = (curr.timestamp / step) * step
            curr_frac = int(floor(delta_v * (curr.timestamp - curr_slot) /
                float(delta_t)))

            self._increase_delta(self.agg, curr_slot, curr_frac)
            self._increase_delta(self.agg, curr_slot - step, prev_frac)

            # if we have some left, try to backfill
            if curr_frac + prev_frac != delta_v:
                missed_slots = range(prev_slot+step, curr_slot, step)
                if delta_t < self.agg.metadata['HEARTBEAT']:
                    missed = delta_v - (curr_frac + prev_frac)
                    assert missed > 0
                    missed_frac = missed / len(missed_slots)
                    missed_rem = missed % (missed_frac * len(missed_slots))
                    for slot in missed_slots:
                        self._increase_delta(self.agg, slot, missed_frac)

                    # distribute the remainder
                    for i in range(missed_rem):
                        self._increase_delta(self.agg, missed_slots[i], 1)
                else:
                    for slot in missed_slots:
                        try:
                            row = self.agg.get(slot)
                        except TSDBVarRangeError:
                            row = self._empty_row(slot)

                        self.agg.invalidate_row(slot)

            prev = curr

        self.agg.metadata['LAST_UPDATE'] = prev.timestamp

        for row in self.agg.select(begin=last_update, flags=ROW_VALID):
            if row.delta != 0:
                row.average = float(row.delta) / step
            else:
                row.average = 0.0
            self.agg.insert(row)

    def update_from_aggregate(self):
        """Update this aggregate from another aggregate."""
        # LAST_UPDATE points to the last step updated

        step = self.agg.metadata['STEP']
        steps_needed = step // self.ancestor.metadata['STEP']
        # XXX what to do if our step isn't divisible by ancestor steps?

        data = self.ancestor.select(
                begin=self.agg.metadata['LAST_UPDATE']
                      + self.ancestor.metadata['STEP'],
                end=self.ancestor.max_valid_timestamp())

        # get all timestamps since the last update
        # fill as many bins as possible
        work = list(itertools.islice(data, 0, steps_needed))

        # if the  first datapoint is on a step boundary we discard it
        # XXX do something more clever?
        if work[0].timestamp % step == 0:
            del work[0]
            work.extend(itertools.islice(data, 0, 1))

        while len(work) == steps_needed:
            slot = ((work[0].timestamp / step) * step) + step

            assert work[-1].timestamp == slot

            valid = 0
            row = Aggregate(slot, ROW_VALID, delta=0, average=None,
                    min=None, max=None)

            for datum in work:
                if datum.flags & ROW_VALID:
                    valid += 1
                    row.delta += datum.delta
    
                    if isNaN(row.min) or datum.delta < row.min:
                        row.min = datum.delta
    
                    if isNaN(row.max) or datum.delta > row.max:
                        row.max = datum.delta
            row.average = row.delta / float(step)
            valid_ratio = float(valid)/float(len(work))

            if valid_ratio < self.agg.metadata['VALID_RATIO']:
                row.invalidate()

            self.agg.insert(row)

            work = list(itertools.islice(data, 0, steps_needed))

        self.agg.metadata['LAST_UPDATE'] = slot


# FIXME These should be refactored into TSDBVarChunk
def read_chunk(chunk, type):
    """Load the data in a chunk into a list."""
    # XXX this doesn't work for Aggregates
    l = []
    f = open(chunk, "rb")
    while True:
        s = f.read(type.size)
        if s == '':
            break

        l.append(type.unpack(s))

    return l

def print_chunk(l, check_valid=True):
    """Print the data in a chunk."""
    for v in l:
        if not check_valid or v.flags & ROW_VALID == 1:
            print v

INTERVAL_SCALARS = {
    's': 1,
    'm': 60,
    'h': 60*60,
    'd': 60*60*24,
    'w': 60*60*24*7
}

INTERVAL_RE = re.compile('^(\d+)(%s)?$' % ('|'.join(INTERVAL_SCALARS.keys())))

def calculate_interval(s):
    """
    Expand intervals expressed as nI, where I is one of:

        s: seconds
        m: minutes
        h: hours
        d: days (24 hours)
        w: weeks (7 days)

    """

    m = INTERVAL_RE.search(s)
    if not m:
        raise InvalidInterval("No match: %s" % s)

    n = int(m.group(1))
    if n < 0:
        raise InvalidInterval("Negative interval: %s" % s)

    scalar = m.group(2)
    if scalar is not None:
        n *= INTERVAL_SCALARS[scalar]

    return n

def _test():
    """
    This is here to clean up the example created by the doc tests:
    >>> import os
    >>> os.system("rm -rf tsdb_example")
    0
    """
    import doctest
    doctest.testmod(verbose=True)

if __name__ == "__main__":
    _test()

