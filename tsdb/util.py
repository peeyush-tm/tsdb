import re
import os

from tsdb.row import Counter32, Counter64
from tsdb.error import InvalidInterval

def write_dict(path, d):
    """Write a dictionary in NAME: VALUE format."""
    f = open(path, "w")

    for key in d:
        f.write(key + ": " + str(d[key]) + "\n")

    f.close()

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

def calculate_slot(ts, step):
    """Calculate which `slot` a given timestamp falls in."""
    return int(ts/step) * step

def rrd_from_tsdb_var(var, begin, rrd_path, heartbeat=None, ds_name=None,
        rows=1000):
    """Given a TSDBVar with aggregates return array to create analogous RRD.

    Return an array that can be passed to rrdtool.create() to create an
    analogous RRD file."""

    name = os.path.basename(var.path)
    step = var.metadata['STEP']
    rrd_file = os.path.join(rrd_path, "%s.rrd" % name)
    rrd_args = [rrd_file, "--start", str(begin), "--step", str(step)]
    if ds_name is None:
        ds_name = name

    if var.type in (Counter32, Counter64):
        if heartbeat is None:
            heartbeat = step*2
        rrd_args.append("DS:%s:COUNTER:%d:0:U" % (ds_name, heartbeat))

    for aggname in var.list_aggregates():
        agg = var.get_aggregate(aggname)
        for agg_func in agg.metadata['AGGREGATES']:
            if agg_func != 'delta':
                rrd_args.append("RRA:%s:0.5:%d:%d" % (agg_func.upper(),
                    agg.metadata['STEP'] / step, rows))

    return rrd_args
