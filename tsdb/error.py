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

class TSDBVarNoValidData(TSDBError):
    """The TSDBVar has no valid data"""
    pass

class UnableToCreateVarChunk(TSDBError):
    """Can't create a chunk"""
    pass

class InvalidMetaData(Exception):
    """Problem with metadata"""
    pass
