import itertools
from math import floor, ceil
from fpconst import isNaN

from tsdb.error import *
from tsdb.row import Aggregate, ROW_VALID, ROW_TYPE_MAP

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
        try:
            if self.ancestor.type == Aggregate:
                self.update_from_aggregate()
            else:
                self.update_from_raw_data(uptime_var=uptime_var)
        except TSDBVarEmpty:
            # not enough data to build aggregate
            pass

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
        min_ts = self.ancestor.min_timestamp()
        if min_ts > last_update:
            last_update = min_ts
            self.agg.metadata['LAST_UPDATE'] = last_update
       
        prev = self.ancestor.get(last_update)

        # XXX this only works for Counter types right now
        for curr in self.ancestor.select(begin=last_update+step,
                flags=ROW_VALID): 
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
        self.agg.flush()

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
            self.agg.flush()
