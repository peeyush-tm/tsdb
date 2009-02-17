"""
TSDB reserves bits 0-7 (the low 8 bits) of the flag field for globally defined flags. 
Bits 8-15 are reserved for Row specific flags.  Bits 16-31 are for application
specific uses.
"""

import struct

ROW_VALID   = 0x0001  # does this row have valid data?
ROW_WRAP    = 0x0002  # was there a wrap between this entry and the previous
ROW_UNWRAP  = 0x0004  # the wrap for this entry was corrected

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

    @classmethod
    def get_invalid_row(klass):
        return klass(0,0,0)

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
            else:
                # XXX not i sure like this
                setattr(self, agg, float('NaN'))

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

    @classmethod
    def get_invalid_row(klass):
        return klass(0,0)

    def invalidate(self):
        TSDBRow.invalidate(self)
        for agg in self.aggregate_order:
            setattr(self, agg, float('NaN'))

ROW_TYPE_MAP = [TSDBRow, Counter32, Counter64, Gauge32, TimeTicks, Aggregate]
