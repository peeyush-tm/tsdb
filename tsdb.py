"""
Prototype implementation of TSDB.

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


os.environ['TZ'] = "UTC"
time.tzset()

class TSDBError(Exception):
    pass

class TSDBAlreadyExistsError(TSDBError):
    pass

class TSDBSetDoesNotExistError(TSDBError):
    pass

class TSDBVarDoesNotExistError(TSDBError):
    pass

class TSDBInvalidName(TSDBError):
    pass

class TSDBNameInUseError(TSDBError):
    pass

class TSDBRow(object):
    """TSDBRows are fixed in size, each subclass defines the size"""
    type_id = 0
    version = 1
    pack_format = "!LL"

    def __init__(self, timestamp, flags, value):
        if self.type_id == 0:
            raise "TSDBRow is an abstract type and can't be instantiated"

        self.timestamp = int(timestamp)
        self.flags = flags
        self.value = value

        if type(self.value) == str:
            self.value = self._from_str(value)

    def pack(self):
        return struct.pack(self.pack_format, self.timestamp, self.flags, self.value)

    @classmethod
    def unpack(klass, s):
        return klass(*struct.unpack(klass.pack_format, s))

    def __str__(self):
        return "ts: %d f: %d v: %d" % (self.timestamp, self.flags, self.value)

class Counter32(TSDBRow):
    type_id = 1
    version = 1
    pack_format = TSDBRow.pack_format + "L"
    size = 12
    
    def _from_str(self, str):
        return int(str)

class Counter64(TSDBRow):
    type_id = 2
    version = 1
    pack_format = TSDBRow.pack_format + "Q"
    size = 16

    def _from_str(self, str):
        return long(str)

class Gauge32(TSDBRow):
    type_id = 3
    version = 1
    pack_format = TSDBRow.pack_format + "L"
    size = 12

    def _from_str(self, str):
        return int(str)

class TimeTicks(TSDBRow):
    type_id = 4
    version = 1
    pack_format = TSDBRow.pack_format + "L"
    size = 12

    def _from_str(self, str):
        return int(str)

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

TYPE_MAP = [TSDBRow, Counter32, Counter64,Gauge32,TimeTicks]

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
    def name(self, timestamp):
        return  "%04d%02d%02d" % time.gmtime(timestamp)[:3]

    @classmethod
    def begin(self, name):
        return calendar.timegm((int(name[:4]), int(name[4:6]), int(name[6:]), 0, 0, 0))

    @classmethod
    def size(self, name, row_size, step):
        begin = self.begin(name)
        (begin_year, begin_month, begin_day) = (int(name[:4]), int(name[4:6]), int(name[6:]))
        (wdays,mdays) = calendar.monthrange(begin_year, begin_month)


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
    def name(self, timestamp):
        return str(timestamp / self.weeksecs)

    @classmethod
    def begin(self, name):
        return int(name) * self.weeksecs

    @classmethod
    def size(self, name, row_size, step):
        return (self.weeksecs/step) * row_size


CHUNK_MAPPER_MAP = [ChunkMapper, YYYYMMChunkMapper, YYYYMMDDChunkMapper, EpochWeekMapper]
  
def write_dict(path, d):
    f = open(path, "w")

    for key in d:
        f.write(key + ": " + str(d[key]) + "\n")

    f.close()

class TSDBBase(object):
    def __init__(self,metadata={}):
        self.metadata = metadata
        self.vars = {}
        self.sets = {}

    def __str__(self):
        return "%s [%s]" % (self.tag, self.path)

    def load_metadata(self):
        f = open(os.path.join(self.path, self.tag), "r")

        for line in f:
            line = line.strip()
            if line.startswith("#"):
                continue
            (var,val) = line.split(':',1)
            val = val.strip()
            if self.metadata_map.has_key(var):
                val = self.metadata_map[var](val)
            self.metadata[var] = val

        f.close()

    def save_metadata(self):
        write_dict(os.path.join(self.path, self.tag), self.metadata)

    def get_metadata(self, var):
        try:
            return(self.metadata[var])
        except:
            return None

    def set_metadata(self, var, val):
        if self.metadata_map.has_key(var):
            val = self.metadata_map[var](val)

        self.metadata[var] = val

    def list_sets(self):
        return filter( \
                lambda x: TSDBSet.is_tsdb_set(os.path.join(self.path, x)),
                os.listdir(self.path))

    def get_set(self, name):
        if not self.sets.has_key(name):
            self.sets[name] = TSDBSet(self, os.path.join(self.path, name))

        return self.sets[name]

    def add_set(self, name):
        prefix = self.path
        set = self
        steps = name.split('/')
        for step in steps[:-1]:
            try:
                set = set.get_set(step)
            except TSDBSetDoesNotExistError:
                TSDBSet.create(prefix, step)
                set = set.get_set(step)

            prefix = os.path.join(prefix, step)

        TSDBSet.create(prefix, steps[-1])
        set = set.get_set(steps[-1])

        return set

    def list_vars(self):
        return filter(TSDBVar.is_tsdb_var, os.listdir(self.path))

    def get_var(self, name):
        if not self.vars.has_key(name):
            self.vars[name] = TSDBVar(self, os.path.join(self.path, name))

        return self.vars[name]

    def add_var(self, name, type, step, chunk_mapper, metadata={}):
        prefix = os.path.dirname(name)
        if prefix is not '':
            try:
                set = self.get_set(prefix)
            except TSDBSetDoesNotExistError:
                self.add_set(prefix)

        TSDBVar.create(self.path, name, type, step, chunk_mapper)
        return self.get_var(name)

    @classmethod
    def is_tag(klass,path):
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
        TSDBBase.__init__(self)
        self.path = path
        self.load_metadata()

    @classmethod
    def is_tsdb(klass, path):
        return klass.is_tag(path)

    @classmethod 
    def create(klass, path, metadata={}):
        if os.path.exists(os.path.join(path, "TSDB")):
            raise TSDBAlreadyExistsError("database already exists")

        metadata["CREATION_TIME"] = time.time()

        if not os.path.exists(path):
            os.mkdir(path)

        write_dict(os.path.join(path, klass.tag), metadata)

        return klass(path)


class TSDBSet(TSDBBase):
    tag = "TSDBSet"
    metadata_map = {}
    def __init__(self, parent, path, metadata={}):
        TSDBBase.__init__(self)
        self.path = path
        self.parent = parent

        if not os.path.exists(path):
            raise TSDBSetDoesNotExistError("TSDBSet does not exist " + path)

        self.load_metadata()


    @classmethod
    def is_tsdb_set(klass, path):
        return self.is_tag(path)

    @classmethod
    def create(klass, root, name, metadata={}):
        path = os.path.join(root, name)
        if os.path.exists(path):
            raise TSDBNameInUseError("%s already exists at %s" % (name, path))

        os.mkdir(path)
        write_dict(os.path.join(path, klass.tag), metadata)

    def lock(self, block=True):
        warnings.warn("locking not implemented yet")

    def unlock(self):
        warnings.warn("locking not implemented yet")

class TSDBVar(TSDBBase):
    """A variable in the database."""

    tag = "TSDBVar"
    metadata_map = {'STEP': int, 'TYPE_ID': int,
            'VERSION': int, 'CHUNK_MAPPER_ID': int}

    def __init__(self, parent, path, use_mmap=False, cache_chunks=False, metadata={}):
        TSDBBase.__init__(self)

        if not os.path.exists(path):
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
        return self.is_tag(path)

    @classmethod
    def create(klass, root, name, vartype, step, chunk_mapper, metadata={}):
        path = os.path.join(root, name)
        if os.path.exists(path):
            raise TSDBNameInUseError("%s already exists at %s" % (name,path))

        if type(vartype) == str:
        name = self.chunk_mapper.name(timestamp)
        name = self.chunk_mapper.name(timestamp)
        name = self.chunk_mapper.name(timestamp)
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

    def _chunk(self, timestamp):
        if not self.cache_chunks:
            for chunk in self.chunks:
                self.chunks[chunk].close()

        name = self.chunk_mapper.name(timestamp)
        if not self.chunks.has_key(name):
            self.chunks[name] = TSDBVarChunk(self, os.path.join(self.path, name), name, timestamp, self.use_mmap)

        return self.chunks[name]

    def select(self, timestamp):
        timestamp = int(timestamp)
        chunk = self._chunk(timestamp)
        return chunk.read_record(timestamp)

    def insert(self, data):
        chunk = self._chunk(data.timestamp)
        return chunk.write_record(data)

    def flush(self):
        for chunk in self.chunks:
            self.chunks[chunk].flush()

    def close(self):
        self.flush()
        for chunk in self.chunks:
            self.chunks[chunk].close()

    def lock(self, block=True):
        """Acquire a write lock."""
        warnings.warn("locking not implemented yet")

    def unlock(self):
        warnings.warn("locking not implemented yet")

class TSDBVarChunk(object):
    def __init__(self, tsdb_var, path, name, timestamp, use_mmap=False):
        if not os.path.exists(path):
            TSDBVarChunk.create(tsdb_var, path, timestamp)

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
    def create(klass, tsdb_var, path, timestamp):
        f = open(path, "w")
        f.write("\0" * tsdb_var.chunk_mapper.size(os.path.basename(path),
            tsdb_var.type.size, tsdb_var.metadata['STEP']))
        f.close()

    def flush(self):
        return self.io.flush()

    def close(self):
        return self.io.close()

    def seek(self, pos, whence=0):
        return self.io.seek(pos,whence)

    def tell(self):
        return self.io.tell()

    def write(self, s):
        return self.io.write(s)

    def read(self, n):
        return self.io.read(n)

    def _offset(self, timestamp):
        o = ((timestamp - self.begin) / self.tsdb_var.metadata['STEP']) * self.tsdb_var.type.size
        return o

    def write_record(self, data):
        if self.use_mmap:
            o = self._offset(data.timestamp)
            self.mmap[o:o+self.tsdb_var.type.size] = data.pack()
        else:
            self.io.seek(self._offset(data.timestamp))
            return self.io.write(data.pack())

    def read_record(self, timestamp):
        if self.use_mmap:
            o = self._offset(timestamp)
            return self.tsdb_var.type.unpack(self.mmap[o:o+self.tsdb_var.type.size])
        else:
            self.io.seek(self._offset(timestamp))
            return self.tsdb_var.type.unpack(self.io.read(self.tsdb_var.type.size))

def read_chunk(chunk, type):
    """Load the data in a chunk into a list."""
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

