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

