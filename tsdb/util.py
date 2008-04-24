import re

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
