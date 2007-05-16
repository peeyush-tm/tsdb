"""
Prototype implementation of TSDB.

Datafile format:

    Header (2048 bytes)
    Records

    Header:

    Byte offset  Description
    -----------  ------------------
    0-3          'TSDB'
    4            file format version
    5            variable type
    5-261        variable name
    262-265      begin in unixtime
    266-269      end in unixtime
    270-273      step in seconds
    274-2047     reserved for expansion

All timestamps are seconds since the epoch in UTC

File structure:

    $ROOT/$set/$varname/$chunk

    ROOT is the database root
    set is the dataset, for example a router
    varname is the variable collected
    chunk is the database chunk, the default strategy is:
        YYYY is four digit year (nevermind the year 10k issues)
        MM is the two digit month

    metadata file:

    $ROOT/$set/$varname/metadata

    EG:

    $ROOT/snv1-mr1/ifInOctets.1/metadata
    $ROOT/snv1-mr1/ifInOctets.1/200704

    header is replicated in each data file for redundancy's sake

"""
#
# XXX add metadata to TSDBSet
# XXX generalize meta data parsing
# XXX no support for sub second steps
#
import struct
import mmap
import os
import os.path
import time
import calendar

#from datetime import tzinfo, timedelta, datetime
#
#class UTC(tzinfo):
#    def utcoffset(self, dt):
#        return timedelta(0)
#    def tzname(self, dt):
#        return "UTC"
#    def dst(self, dt):
#        return timedelta(0)
#

os.environ['TZ'] = "UTC"
time.tzset()

#file_format_version = 1
#class Header(object):
#    def pack(self):
#        return struct.pack("!4sBBLL1778s", "TSDB", file_format_version, self.type,
#                self.begin, self.end, self.step, "\0" * 1774)

class TSDBRow(object):
    """TSDBRows are fixed in size, each subclass defines the size"""
    type_id = 0
    version = 1
    pack_format = "!LL"

    def __init__(self, timestamp, flags, value):
        if self.type_id == 0:
            raise "TSDBRow is an abstract type and can't be instantiated"

        self.timestamp = timestamp
        self.flags = flags
        self.value = value

    def pack(self):
        return struct.pack(self.pack_format, self.timestamp, self.flags, self.value)

    @classmethod
    def unpack(klass, s):
        return klass(*struct.unpack(klass.pack_format, s))

    @classmethod
    def size(klass):
        return struct.calcsize(klass.pack_format)

class Counter32(TSDBRow):
    type_id = 1
    version = 1
    pack_format = TSDBRow.pack_format + "L"

class Counter64(TSDBRow):
    type_id = 2
    version = 1
    pack_format = TSDBRow.pack_format + "Q"

#
# XXX not sure we really need the flags
# if timestamp is 0, the row is invalid
# should we just handle wraps as we insert?
#
ROW_VALID   = 0x0001  # does this row have valid data?
ROW_WRAP    = 0x0002  # was there a wrap between this entry and the previous
ROW_UNWRAP  = 0x0004  # the wrap for this entry was corrected

# what row cases we might see
#
# wrap and we have the next entry
# wrap and then a gap in the data
# gap in the data:
#  small gap: rates are still likely valid
#  large gap: rates are possibly not valid

TYPE_MAP = [TSDBRow, Counter32, Counter64]

"""A ChunkMapper takes a timestamp in seconds since the epoch and returns a
string which is the name of the chunk containing that timestamp"""

class ChunkMapper(object):
    chunk_mapper_id = 0
    def __init__(self):
        if self.chunk_mapper_id == 0:
            raise "ChunkMapper is an abstract class"

    def name(self, timestamp):
        raise NotImplementedError("name")

    def size(self):
        raise NotImplementedError("name")

class YYYYMMChunkMapper(ChunkMapper):
    chunk_mapper_id = 1

    @classmethod
    def name(self, timestamp):
        return  "%04d%02d" % time.gmtime(timestamp)[:2]

    @classmethod
    def begin(self, name):
        return calendar.timegm((int(name[:4]), int(name[-2:]), 1, 0, 0, 0))

    @classmethod
    def size(self, name, row_size, step):
        begin = self.begin(name)
        (begin_year, begin_month) = (int(name[:4]), int(name[-2:]))
        if begin_month == 12:
            end_year = begin_year + 1
            end_month = 1
        else:
            end_year = begin_year
            end_month = begin_month + 1

        end = calendar.timegm((end_year, end_month, 1, 0, 0, 0))

        return ((end - begin)/step)*row_size

CHUNKMAPPER_MAP = [ChunkMapper, YYYYMMChunkMapper]
    
class TSDB(object):
    """A time series data base (TSDB).

    Each TSDS is made up of collection of sets.  Each set contains one or more
    variables.
    """

    def __init__(self, path):
        self.path = path
        f = open(os.path.join(path, "TSDB"))
        for line in f:
            line = line.strip()
            if line.startswith("#"):
                continue
            (var,val) = line.split(":")
            val = val.strip()
            if var == "chunk_mapper_id":
                val = int(val)
            setattr(self, var, val)
        f.close()
        self.chunk_mapper = CHUNKMAPPER_MAP[self.chunk_mapper_id]

    @classmethod 
    def create(klass, path, chunk_mapper):
        if os.path.exists(os.path.join(path, "TSDB")):
            raise "database already exists"

        if not os.path.exists(path):
            os.mkdir(path)

        f = open(os.path.join(path, "TSDB"), "w")
        print >>f, "chunk_mapper_id:", chunk_mapper.chunk_mapper_id
        print >>f, "creation_time:", time.time()
        f.close()
        return klass(path)

    def list_sets(self):
        return filter(lambda x: os.path.isdir(os.path.join(self.path,x)), os.listdir(self.path))

    def get_set(self, name):
        return TSDBSet(self, os.path.join(self.path, name))

    def add_set(self, name):
        TSDBSet.create(self.path, name)
        return self.get_set(name)


class TSDBSet(object):
    def __init__(self, tsdb, path):
        if not os.path.exists(path):
            raise "TSDBSet does not exist:", path
        self.tsdb = tsdb
        self.path = path
        self.metadata = os.path.join(self.path, "TSDBVar")

    @classmethod
    def create(self, root, name):
        path = os.path.join(root, name)
        if os.path.exists(path):
            raise "set %s already exists" % set

        os.mkdir(path)

    def list_vars(self):
        return filter(lambda x: os.path.isdir(os.path.join(self.path,x)), os.listdir(self.path))

    def add_var(self, name, type, step):
        TSDBVar.create(self.path, name, type, step)
        return self.get_var(name)

    def get_var(self, name):
        return TSDBVar(self, os.path.join(self.path, name))

class TSDBVar(object):
    """A variable in the database."""
    def __init__(self, tsdb_set, path):
        if not os.path.exists(path):
            raise "TSDBVar does not exist:", path

        self.tsdb_set = tsdb_set
        self.path = path
        self.metadata = os.path.join(self.path, "TSDBVar")
        if not os.path.exists(self.metadata):
            raise "TSDBVar %s has no metadata"

        f = open(self.metadata, "r")
        for line in f:
            line = line.strip()
            if line.startswith("#"):
                continue
            (var,val) = line.split(":")
            val = val.strip()
            if var == "step" or var == "type_id" or var == "version":
                val = int(val)
            setattr(self, var, val)
        f.close()

        self.type = TYPE_MAP[self.type_id]
        self.chunks = {} # memory resident chunks
        self.chunk_mapper = self.tsdb_set.tsdb.chunk_mapper

    @classmethod
    def create(klass, root, name, type, step, extra={}):
        path = os.path.join(root, name)
        if os.path.exists(path):
            raise "TSDBVar %s already exists" % name

        os.mkdir(path)
        f = open(os.path.join(root,name,"TSDBVar"),"w")
        print >>f, "name:", name
        print >>f, "type_id:", type.type_id
        print >>f, "version:", type.version
        print >>f, "step:", step
        print >>f, "creation_time:", time.time()
        for key in extra:
            print >>f, key+":", extra[key]
        f.close()

    def _chunk(self, timestamp):
        name = self.chunk_mapper.name(timestamp)
        if not self.chunks.has_key(name):
            self.chunks[name] = TSDBVarChunk(self, os.path.join(self.path, name), name, timestamp)

        return self.chunks[name]

    def select(self, timestamp):
        chunk = self._chunk(timestamp)
        return chunk.read_record(timestamp)

    def insert(self, data):
        chunk = self._chunk(data.timestamp)
        return chunk.write_record(data)

    def flush(self):
        pass

    def flush_set(self, set):
        pass

    def close(self):
        pass

    def close_set(self, set):
        pass


class TSDBVarChunk(object):
    def __init__(self, tsdb_var, path, name, timestamp):
        if not os.path.exists(path):
            TSDBVarChunk.create(tsdb_var, path, timestamp)

        self.tsdb_var = tsdb_var
        self.path = path
        self.name = name

#        self.fd = os.open(path, os.O_RDWR)
#        self.mmap = mmap.mmap(self.fd, 0) # defaults are OK for other fields
        self.file = open(self.path, "r+")
        self.begin = tsdb_var.chunk_mapper.begin(os.path.basename(self.path))

    @classmethod
    def create(klass, tsdb_var, path, timestamp):
        f = open(path, "w")
        f.write("\0" * tsdb_var.chunk_mapper.size(os.path.basename(path),
            tsdb_var.type.size(), tsdb_var.step))
        f.close()

    def flush(self):
        return self.file.flush()

    def seek(self, pos, whence=0):
        return self.file.seek(pos,whence)

    def tell(self):
        return self.file.tell()

    def write(self, s):
        return self.file.write(s)

    def read(self, n):
        return self.file.read(n)

    def _offset(self, timestamp):
        return ((timestamp - self.begin) / self.tsdb_var.step) * self.tsdb_var.type.size()

    def write_record(self, data):
        self.file.seek(self._offset(data.timestamp))
        return self.file.write(data.pack())

    def read_record(self, timestamp):
        self.file.seek(self._offset(timestamp))
        return self.tsdb_var.type.unpack(self.file.read(self.tsdb_var.type.size()))
