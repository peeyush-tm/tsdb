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
import itertools
import time
from math import floor, ceil

from fpconst import isNaN

from tsdb.error import *
from tsdb.row import Aggregate, ROW_VALID, ROW_TYPE_MAP
from tsdb.chunk_mapper import CHUNK_MAPPER_MAP
from tsdb.base import TSDBBase
from tsdb.util import write_dict, calculate_interval

__version__ = ""

os.environ['TZ'] = "UTC"
time.tzset()

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

        if not metadata.has_key('LAST_UPDATE'):
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
            'AGGREGATES': list, 'LAST_UPDATE': int, 'VALID_RATIO': float,
            'HEARTBEAT': int}

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

        self.type = ROW_TYPE_MAP[self.metadata['TYPE_ID']]
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
            vartype = ROW_TYPE_MAP[vartype]

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
            val = chunk.read_row(timestamp)
        except TSDBVarChunkDoesNotExistError:
            val = self.type.get_invalid_row()

        if not val.flags & ROW_VALID:
            # if row isn't valid the timestamp is 0, so we fix that
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
        with ROW_VALID set.

        Diagram of the relationship between the prev and curr elements in the
        main loop:

        prev_slot          curr_slot
        |                  |
        v                  v
        +----------+       +----------+
        |   prev   | . . . |   curr   |
        +----------+       +----------+
             ^                   ^
             |                   |
             prev.timestamp      curr.timestamp
             |                   |
             |<---- delta_t ---->|
        """

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
            prev_slot = (prev.timestamp / step) * step
            curr_slot = (curr.timestamp / step) * step

            if self.ancestor.type.can_rollover and delta_v < 0:
                assert uptime_var is not None 
                delta_uptime = uptime_var.get(curr.timestamp).value - \
                        uptime_var.get(prev.timestamp).value

                if delta_uptime < 0:
                    # this is a reset
                    delta_v = curr.value
                else:
                    delta_v = self.ancestor.type.rollover(delta_v)

            # XXX: this is a kludge:
            rate = float(delta_v) / float(delta_t)
            if rate > 12000000000 / 8:
                print "WARNING: bad data: ", rate, prev, curr
                prev = curr
                continue

            assert delta_v >= 0
            #
            # tests for edge cases: rollover, invalid, large gaps in data
            # not sure how to properly invalidate individual rows

            # allocate a portion of this data to a given bin
            prev_frac = int( floor(
                        delta_v * (prev_slot+step - prev.timestamp)
                        / float(delta_t)
                    ))

            curr_frac = int( ceil(
                        delta_v * (curr.timestamp - curr_slot)
                        / float(delta_t)
                    ))

            if delta_t > self.agg.metadata['HEARTBEAT']:
                for slot in range(prev_slot, curr_slot, step):
                    try:
                        row = self.agg.get(slot)
                    except TSDBVarRangeError:
                        row = self._empty_row(self.agg, slot)

                    row.invalidate()
                    self.agg.insert(row)

                self._increase_delta(self.agg, curr_slot, curr_frac)
                prev = curr
                continue

            self._increase_delta(self.agg, curr_slot, curr_frac)
            self._increase_delta(self.agg, prev_slot, prev_frac)

            # if we have some left, try to backfill
            if curr_frac + prev_frac != delta_v:
                missed_slots = range(prev_slot+step, curr_slot, step)
                missed = delta_v - (curr_frac + prev_frac)
                assert missed > 0
                missed_frac = missed / len(missed_slots)
                missed_rem = missed % (missed_frac * len(missed_slots))
                for slot in missed_slots:
                    self._increase_delta(self.agg, slot, missed_frac)

                # distribute the remainder
                for i in range(missed_rem):
                    self._increase_delta(self.agg, missed_slots[i], 1)

            prev = curr


        for row in self.agg.select(begin=last_update, flags=ROW_VALID):
            if row.delta != 0:
                row.average = float(row.delta) / step
            else:
                row.average = 0.0
            self.agg.insert(row)

        self.agg.metadata['LAST_UPDATE'] = prev.timestamp
        self.agg.save_metadata()
        self.agg.flush()

    def update_from_aggregate(self):
        """Update this aggregate from another aggregate."""
        # LAST_UPDATE points to the last step updated
        print "From agg:", self.agg

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

        slot = None
        while len(work) == steps_needed:
            slot = ((work[0].timestamp / step) * step) #+ step

#            assert work[-1].timestamp == slot

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
       
        if slot is not None:
            self.agg.metadata['LAST_UPDATE'] = slot
            self.agg.save_metadata()
            self.agg.flush()

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

