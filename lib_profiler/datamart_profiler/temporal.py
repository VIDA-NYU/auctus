import collections
import logging
import pandas


logger = logging.getLogger(__name__)


temporal_aggregation_keys = {
    'year': '%Y',
    'month': '%Y-%m',
    'week': lambda dt: (
        # Simply using "%Y-%W" doesn't work at year boundaries
        # Map each timestamp to the first day of its week
        (dt - pandas.Timedelta(days=dt.weekday())).strftime('%Y-%m-%d')
    ),
    'day': '%Y-%m-%d',
    'hour': '%Y-%m-%d %H',
    'minute': '%Y-%m-%d %H:%M',
    'second': '%Y-%m-%d %H:%M:%S',
}


def get_temporal_resolution(values):
    """Returns the resolution of the temporal attribute.
    """

    # Python 3.7+ iterates on dict in insertion order
    for resolution, key in temporal_aggregation_keys.items():
        counts = collections.defaultdict(collections.Counter)
        if isinstance(key, str):
            for value in values:
                bin = value.strftime(key)
                counts[bin][value] += 1
        else:
            for value in values:
                bin = key(value)
                counts[bin][value] += 1

        avg_per_bin = sum(len(v) for v in counts.values()) / len(counts)
        if avg_per_bin < 1.05:
            # 5 % error tolerated
            return resolution

    return 'second'
