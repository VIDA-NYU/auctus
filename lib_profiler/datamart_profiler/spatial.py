import collections
from dataclasses import dataclass
import json
import logging
import math
import numpy
import numpy.random
import prometheus_client
import re
import requests
from sklearn.cluster import KMeans
from sklearn.exceptions import ConvergenceWarning
from sklearn.neighbors._kd_tree import KDTree
import time
import typing
from urllib.parse import urlencode

from .warning_tools import ignore_warnings


logger = logging.getLogger(__name__)


N_RANGES = 3
MIN_RANGE_SIZE = 0.10  # 10%

SPATIAL_RANGE_DELTA_LONG = 0.0001
SPATIAL_RANGE_DELTA_LAT = 0.0001

MAX_ADDRESS_LENGTH = 90  # 90 characters
MAX_NOMINATIM_REQUESTS = 200
NOMINATIM_BATCH_SIZE = 20
NOMINATIM_MIN_SPLIT_BATCH_SIZE = 2  # Batches >=this are divided on failure

LATITUDE = ('latitude', 'lat', 'ycoord', 'y_coord')
LONGITUDE = ('longitude', 'long', 'lon', 'lng', 'xcoord', 'x_coord')

MAX_WRONG_LEVEL_ADMIN = 0.10  # 10%


PROM_NOMINATIM_REQS = prometheus_client.Counter(
    'profile_nominatim_reqs', "Queries to Nominatim",
)
PROM_NOMINATIM_REQ_TIME = prometheus_client.Histogram(
    'profile_nominatim_req_seconds', "Time for Nominatim to answer a query",
)


def get_spatial_ranges(values):
    """Build a small number (3) of bounding boxes from lat/long points.

    This performs K-Means clustering, returning a maximum of 3 clusters as
    bounding boxes.
    """

    clustering = KMeans(n_clusters=min(N_RANGES, len(values)),
                        random_state=0)
    with ignore_warnings(ConvergenceWarning):
        clustering.fit(values)
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

        cluster.sort(key=lambda p: p[0])
        min_idx = int(0.05 * len(cluster))
        max_idx = int(0.95 * len(cluster))
        min_lat = cluster[min_idx][0]
        max_lat = cluster[max_idx][0]
        cluster.sort(key=lambda p: p[1])
        min_long = cluster[min_idx][1]
        max_long = cluster[max_idx][1]
        ranges.append([
            [min_long, max_lat],
            [max_long, min_lat],
        ])
        sizes.append(len(cluster))
    ranges.sort()
    logger.info("Ranges: %r", ranges)
    logger.info("Sizes: %r", sizes)

    # Lucene needs shapes to have an area for tessellation (no point or line)
    for rg in ranges:
        if rg[0][0] == rg[1][0]:
            rg[0][0] -= SPATIAL_RANGE_DELTA_LONG
            rg[1][0] += SPATIAL_RANGE_DELTA_LONG
        if rg[0][1] == rg[1][1]:
            rg[0][1] += SPATIAL_RANGE_DELTA_LAT
            rg[1][1] -= SPATIAL_RANGE_DELTA_LAT

    # Convert to Elasticsearch syntax
    ranges = [{'range': {'type': 'envelope',
                         'coordinates': coords}}
              for coords in ranges]
    return ranges


def normalize_latlong_column_name(name, substrings):
    """Find the remainder of the column name after removing a substring.

    This goes over the substrings in order and removes the first it finds from
    the name. You should therefore put the more specific substrings first and
    the shorter ones last. For example, this is used to turn both
    ``"cab_latitude_from"`` and ``"cab_longitude_from"`` into ``"cab__from"``
    which can then be matched.
    """
    name = name.strip().lower()
    for substr in substrings:
        idx = name.find(substr)
        if idx >= 0:
            name = name[:idx] + name[idx + len(substr):]
            break
    return name


@dataclass
class LatLongColumn(object):
    index: int
    name: str
    annot_pair: typing.Optional[str]


def pair_latlong_columns(columns_lat, columns_long):
    """Go through likely latitude and longitude columns and finds pairs.

    This tries to find columns that match apart from latitude and longitude
    keywords (e.g. `longitude`, `long`, `lon`).
    """
    # Normalize latitude column names
    normalized_lat = {}
    for i, col in enumerate(columns_lat):
        # check if a pair was defined by the user (human-in-the-loop)
        name = col.annot_pair
        if name is None:
            # Use normalized column name
            name = normalize_latlong_column_name(col.name, LATITUDE)
        normalized_lat[name] = i

    # Go over normalized longitude column names and try to match
    pairs = []
    missed_long = []
    for col in columns_long:
        # check if a pair was defined by the user (human-in-the-loop)
        name = col.annot_pair
        if name is None:
            # Use normalized column name
            name = normalize_latlong_column_name(col.name, LONGITUDE)
        if name in normalized_lat:
            pairs.append((
                columns_lat[normalized_lat.pop(name)],
                col,
            ))
        else:
            missed_long.append(col.name)

    # Gather missed columns
    missed_lat = [columns_lat[i].name for i in sorted(normalized_lat.values())]

    return pairs, (missed_lat, missed_long)


_re_loc = re.compile(
    r'\('
    r'(-?[0-9]{1,3}\.[0-9]{1,15})'
    r'(?:,| |(?:, ))'
    r'(-?[0-9]{1,3}\.[0-9]{1,15})'
    r'\)$'
)


def _parse_point(value, latlong):
    m = _re_loc.search(value)
    if m is not None:
        try:
            x = float(m.group(1))
            y = float(m.group(2))
        except ValueError:
            return None
        if latlong:
            x, y = y, x
        if -180.0 < x < 180.0 and -90.0 < y < 90.0:
            return y, x


def parse_wkt_column(values, latlong=False):
    """Parse a pandas.Series of points in WKT format or similar "(long, lat)".

    :param latlong: If False (the default), read ``(long, lat)`` format. If
        True, read ``(lat, long)``.
    :returns: A list of ``(lat, long)`` pairs
    """
    # Parse points
    values = values.apply(_parse_point, latlong=latlong)
    # Drop NaN values
    values = values.dropna(axis=0)

    return list(values)


_nominatim_session = requests.Session()


def nominatim_query(url, *, q):
    url = url.rstrip('/')
    res = start = end = None  # Avoids warnings
    for i in range(5):
        if i > 0:
            time.sleep(1)
        PROM_NOMINATIM_REQS.inc()  # Count all requests
        start = time.perf_counter()
        if isinstance(q, (tuple, list)):
            # Batch query
            res = _nominatim_session.get(
                url +
                '/search?' +
                urlencode({
                    'batch': json.dumps([{'q': qe} for qe in q]),
                    'format': 'jsonv2',
                }),
            )
        else:
            # Normal query
            res = _nominatim_session.get(
                url +
                '/search?' +
                urlencode({'q': q, 'format': 'jsonv2'}),
            )
        end = time.perf_counter()
        if res.status_code not in (502, 503, 504):
            break
    res.raise_for_status()
    # Record time for successful request
    PROM_NOMINATIM_REQ_TIME.observe(end - start)
    if not res.headers['Content-Type'].startswith('application/json'):
        raise requests.HTTPError(
            "Response is not JSON for URL: %s" % res.url,
            response=res,
        )
    if isinstance(q, (tuple, list)):
        return res.json()['batch']
    else:
        return res.json()


def _nominatim_batch(url, batch, locations, cache):
    try:
        locs = nominatim_query(url, q=list(batch.keys()))
    except requests.HTTPError as e:
        if (
            e.response.status_code in (500, 414)
            and len(batch) >= max(2, NOMINATIM_MIN_SPLIT_BATCH_SIZE)
        ):
            # Try smaller batch size
            batch_list = list(batch.items())
            mid = len(batch) // 2
            return (
                _nominatim_batch(url, dict(batch_list[:mid]), locations, cache)
                +
                _nominatim_batch(url, dict(batch_list[mid:]), locations, cache)
            )
        raise e from None

    not_found = 0
    for location, (value, count) in zip(locs, batch.items()):
        if location:
            loc = (
                float(location[0]['lat']),
                float(location[0]['lon']),
            )
            cache[value] = loc
            locations.extend([loc] * count)
        else:
            cache[value] = None
            not_found += count
    batch.clear()
    return not_found


def nominatim_resolve_all(url, array, max_requests=MAX_NOMINATIM_REQUESTS):
    cache = {}
    locations = []
    not_found = 0  # Unique locations not found
    non_empty = 0
    start = time.perf_counter()
    processed = 0
    batch = {}

    for processed, value in enumerate(array):
        value = value.strip()
        if not value:
            continue
        non_empty += 1

        if len(value) > MAX_ADDRESS_LENGTH:
            continue
        elif value in cache:
            if cache[value] is not None:
                locations.append(cache[value])
        elif value in batch:
            batch[value] += 1
        else:
            batch[value] = 1
            if len(batch) == NOMINATIM_BATCH_SIZE:
                not_found += _nominatim_batch(url, batch, locations, cache)
                if len(cache) >= max_requests:
                    break

    if batch and len(cache) < max_requests:
        not_found += _nominatim_batch(url, batch, locations, cache)

    logger.info(
        "Performed %d Nominatim queries in %fs (%d hits). Found %d/%d",
        len(cache),
        time.perf_counter() - start,
        len(cache) - not_found,
        len(locations),
        processed,
    )
    return locations, non_empty


def disambiguate_admin_areas(admin_areas):
    """This takes admin areas resolved from names and tries to disambiguate.

    Each name in the input will have been resolved to multiple possible areas,
    making the input a list of list. We want to build a simple list, where
    each name has been resolved to the most likely area.

    We choose so that all the areas are of the same level (e.g. all countries,
    or all states, but not a mix of counties and states), and if possible all
    in the same parent area (for example, states of the same country, or
    counties in states of the same country).
    """
    # Count possible options
    options = collections.Counter()
    for candidates in admin_areas:
        # Count options from the same list of candidates only once
        options_for_entry = set()
        for area in candidates:
            level = area.type.value
            area = area.get_parent_area()
            while area:
                options_for_entry.add((level, area))
                area = area.get_parent_area()
            options_for_entry.add((level, None))
        options.update(options_for_entry)

    # Throw out options with too few matches
    threshold = (1.0 - MAX_WRONG_LEVEL_ADMIN) * len(admin_areas)
    threshold = max(3, threshold)
    options = [
        (option, count) for (option, count) in options.items()
        if count >= threshold
    ]
    if not options:
        return None

    # Find best option
    (level, common_parent), _ = min(
        options,
        # Order:
        key=lambda entry: (  # lambda ((level, parent_area), count):
            # - by ascending level (prefer recognizing as a list of countries
            #   over a list of states), then
            entry[0][0],
            # - by descending level of the common parent (prefer a list of
            #   counties in the same state over counties merely in the same
            #   country over counties in different countries), then
            -(entry[0][1].type.value if entry[0][1] is not None else -1),
            # - by descending count
            -entry[1],
        )
    )
    if common_parent is None:
        common_admin = None
    else:
        common_admin = common_parent.levels[common_parent.type.value]

    # Build the result
    result = []
    for candidates in admin_areas:
        for area in candidates:
            if (
                area.type.value == level and (
                    common_parent is None or
                    area.levels[common_parent.type.value] == common_admin
                )
            ):
                result.append(area)
                break

    return level, result


GEOHASH_CHARS = '0123456789bcdefghjkmnpqrstuvwxyz'
assert len(GEOHASH_CHARS) == 32
GEOHASH_CHAR_VALUES = {c: i for i, c in enumerate(GEOHASH_CHARS)}


def bits_to_chars(bits, base_bits):
    result = []
    i = 0
    while i + base_bits <= len(bits):
        char = 0
        for j in range(base_bits):
            char = (char << 1) | bits[i + j]
        result.append(GEOHASH_CHARS[char])
        i += base_bits

    return ''.join(result)


def chars_to_bits(chars, base_bits):
    for char in chars:
        char = GEOHASH_CHAR_VALUES[char]
        for i in reversed(range(base_bits)):
            yield (char >> i) & 1


def location_to_bits(point, base, precision):
    latitude, longitude = point

    # Compute the number of bits we need
    base_bits = base.bit_length() - 1
    if 2 ** base_bits != base:
        raise ValueError("Base is not a power of 2")
    precision_bits = base_bits * precision

    lat_range = -90.0, 90.0
    long_range = -180.0, 180.0
    bits = []
    while len(bits) < precision_bits:
        mid = (long_range[0] + long_range[1]) / 2.0
        if longitude > mid:
            bits.append(1)
            long_range = mid, long_range[1]
        else:
            bits.append(0)
            long_range = long_range[0], mid

        mid = (lat_range[0] + lat_range[1]) / 2.0
        if latitude > mid:
            bits.append(1)
            lat_range = mid, lat_range[1]
        else:
            bits.append(0)
            lat_range = lat_range[0], mid
    return bits


def hash_location(point, base=32, precision=16):
    """Hash coordinates into short strings usable for prefix search.

    If base=32, this gives Geohash strings (each level splits cells into 32).

    If base=4, this makes a quadtree (each level split cells into 4 quadrants).
    """
    base_bits = base.bit_length() - 1

    # Build the bitstring
    bits = location_to_bits(point, base, precision)

    # Encode the bitstring
    return bits_to_chars(bits, base_bits)


def decode_hash(hash, base=32):
    """Turn a hash back into a rectangle.

    :returns: ``(min_lat, max_lat, min_long, max_long)``
    """
    base_bits = base.bit_length() - 1
    if 2 ** base_bits != base:
        raise ValueError("Base is not a power of 2")

    lat_range = -90.0, 90.0
    long_range = -180.0, 180.0
    next_long = True
    for bit in chars_to_bits(hash, base_bits):
        if next_long:
            mid = (long_range[0] + long_range[1]) / 2.0
            if bit:
                long_range = mid, long_range[1]
            else:
                long_range = long_range[0], mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2.0
            if bit:
                lat_range = mid, lat_range[1]
            else:
                lat_range = lat_range[0], mid
        next_long = not next_long

    return (
        lat_range[0], lat_range[1],
        long_range[0], long_range[1],
    )


def bitrange(from_bits, to_bits):
    bits = list(from_bits)
    while bits != to_bits:
        yield bits
        for i in range(len(bits) - 1, -1, -1):
            if bits[i] == 0:
                bits[i] = 1
                break
            else:
                bits[i] = 0
    yield bits


class Geohasher(object):
    def __init__(self, *, number, base=4, precision=16):
        self.number = number
        self.base = base
        self.precision = precision

        self.tree_root = [0, {}]
        self.number_at_level = [0] * (precision)

    def add_points(self, points):
        for point in points:
            geohash = hash_location(point, self.base, self.precision)
            # Add this hash to the tree
            node = self.tree_root
            for level, key in enumerate(geohash):
                node[0] += 1
                try:
                    node = node[1][key]
                except KeyError:
                    new_node = [0, {}]
                    node[1][key] = new_node
                    node = new_node
                    self.number_at_level[level] += 1

                    # If this level has too many nodes, stop building it
                    if self.number_at_level[level] > self.number:
                        self.precision = level
                        break
            node[0] += 1

    def add_aab(self, box):
        base_bits = self.base.bit_length() - 1

        min_long, max_long, min_lat, max_lat = box
        min_bits = location_to_bits(
            (min_lat, min_long), self.base, self.precision,
        )
        min_long_bits = min_bits[0::2]
        min_lat_bits = min_bits[1::2]
        max_bits = location_to_bits(
            (max_lat, max_long), self.base, self.precision,
        )
        max_long_bits = max_bits[0::2]
        max_lat_bits = max_bits[1::2]

        self.tree_root[0] += 1
        level = 1
        while level <= self.precision:
            n_long_bits = math.ceil(level * base_bits / 2)
            n_lat_bits = math.floor(level * base_bits / 2)
            for long_bits in bitrange(
                    min_long_bits[:n_long_bits],
                    max_long_bits[:n_long_bits],
            ):
                for lat_bits in bitrange(
                        min_lat_bits[:n_lat_bits],
                        max_lat_bits[:n_lat_bits],
                ):
                    bits = [0] * (n_long_bits + n_lat_bits)
                    bits[0::2] = long_bits
                    bits[1::2] = lat_bits
                    geohash = bits_to_chars(bits, base_bits)

                    # Add this hash to the tree
                    node = self.tree_root
                    for lvl, key in enumerate(geohash):
                        try:
                            node = node[1][key]
                        except KeyError:
                            new_node = [0, {}]
                            node[1][key] = new_node
                            node = new_node
                            self.number_at_level[lvl] += 1
                    node[0] += 1

                    if self.number_at_level[level - 1] > self.number:
                        self.precision = level - 1
                        break

            level += 1

    def get_hashes(self):
        # Reconstruct the hashes at this level
        hashes = []

        def add_node(prefix, node, level):
            if level == self.precision:
                hashes.append((prefix, node[0]))
                return
            for k, n in node[1].items():
                add_node(prefix + k, n, level + 1)

        add_node('', self.tree_root, 0)
        return hashes

    def get_hashes_json(self):
        hashes = self.get_hashes()
        return [
            {
                'hash': h,
                'number': n,
            }
            for h, n in hashes
        ]

    @property
    def total(self):
        return self.tree_root[0]


def median_smallest_distance(points, tree=None):
    """Median over all points of the distance to their closest neighbor.

    This gives an idea of the "grid size" of a point dataset.
    """
    points = numpy.array(points)
    if tree is None:
        # points = numpy.unique(points, axis=0)  # Too slow
        points = numpy.array(list(set(tuple(p) for p in points)))
        tree = KDTree(points)

    # Get the minimum distances to neighbors for a sample of points
    rnd = numpy.random.RandomState(89)
    sample_size = min(len(points), 100)
    sample_idx = rnd.choice(len(points), sample_size, replace=False)
    sample = points[sample_idx]
    distances, _ = tree.query(sample, k=2, return_distance=True)

    # Return the median of that
    return numpy.median(distances[:, 1])
