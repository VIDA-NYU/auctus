import logging
import math
import numpy
from sklearn.cluster import KMeans
from sklearn.exceptions import ConvergenceWarning

from .warning_tools import ignore_warnings


logger = logging.getLogger(__name__)


N_RANGES = 3
MIN_RANGE_SIZE = 0.1  # 10%


def mean_stddev(array):
    """Compute the mean (average) and standard deviation of a numerical array.
    """
    total = 0
    count = 0
    for elem in array:
        if elem is not None:
            total += elem
            count += 1
    mean = total / count if count > 0 else 0

    total = 0
    for elem in array:
        if elem is not None:
            elem = elem - mean
            total += elem * elem
    stddev = math.sqrt(total / count) if count > 0 else 0

    return mean, stddev


def get_numerical_ranges(values):
    """
    Retrieve the numeral ranges given the input (timestamp, integer, or float).

    This performs K-Means clustering, returning a maximum of 3 ranges.
    """

    if not len(values):
        return []

    logger.info("Computing numerical ranges, %d values", len(values))

    clustering = KMeans(n_clusters=min(N_RANGES, len(values)),
                        random_state=0)
    values_array = numpy.array(values).reshape(-1, 1)
    with ignore_warnings(ConvergenceWarning):
        clustering.fit(values_array)
    logger.info("K-Means clusters: %r", list(clustering.cluster_centers_))

    # Compute confidence intervals for each range
    ranges = []
    sizes = []
    for rg in range(N_RANGES):
        cluster = [values[i]
                   for i in range(len(values))
                   if clustering.labels_[i] == rg]
        if not cluster:
            continue

        # Eliminate clusters of outliers
        if len(cluster) < MIN_RANGE_SIZE * len(values):
            continue

        cluster.sort()
        min_idx = int(0.05 * len(cluster))
        max_idx = int(0.95 * len(cluster))
        ranges.append([
            cluster[min_idx],
            cluster[max_idx],
        ])
        sizes.append(len(cluster))
    ranges.sort()
    logger.info("Ranges: %r", ranges)
    logger.info("Sizes: %r", sizes)

    # Convert to Elasticsearch syntax
    ranges = [{'range': {'gte': float(rg[0]), 'lte': float(rg[1])}}
              for rg in ranges]
    return ranges
