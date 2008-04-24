import calendar
import time

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

__all__ = CHUNK_MAPPER_MAP 
