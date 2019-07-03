from datetime import datetime
from dateutil.tz import UTC
import unittest
from unittest.mock import call, patch

from datamart_profiler import pair_latlong_columns, \
    normalize_latlong_column_name
from datamart_profiler.identify_types import parse_date


class TestLatlongSelection(unittest.TestCase):
    def test_normalize_name(self):
        """Test normalizing column names."""
        self.assertEqual(
            normalize_latlong_column_name('latitude', 'latitude', 'lat'),
            '',
        )
        self.assertEqual(
            normalize_latlong_column_name('Place_Latitude', 'latitude', 'lat'),
            'place_',
        )
        self.assertEqual(
            normalize_latlong_column_name('start_Lat_deg', 'latitude', 'lat'),
            'start__deg',
        )

    def test_pairing(self):
        """Test pairing latitude and longitude columns by name."""
        with patch('datamart_profiler.logger') as mock_warn:
            pairs = pair_latlong_columns(
                [
                    ('Pickup_latitude', 1),
                    ('lat', 7),
                    ('dropoff_latitude', 2),
                    ('latitude_place', 8),
                ],
                [
                    ('long', 5),
                    ('dropoff_Longitude', 3),
                    ('pickup_longitude', 4),
                    ('other_Longitude', 6),
                ],
            )
        self.assertEqual(
            pairs,
            [
                (('lat', 7), ('long', 5)),
                (('dropoff_latitude', 2), ('dropoff_Longitude', 3)),
                (('Pickup_latitude', 1), ('pickup_longitude', 4)),
            ],
        )
        self.assertEqual(
            mock_warn.warning.call_args_list,
            [
                call("Unmatched latitude columns: %r", ['latitude_place']),
                call("Unmatched longitude columns: %r", ['other_Longitude']),
            ],
        )


class TestDates(unittest.TestCase):
    def test_parse(self):
        """Test parsing dates."""
        self.assertEqual(
            parse_date('Monday July 1, 2019'),
            datetime(2019, 7, 1, tzinfo=UTC),
        )
        self.assertEqual(
            parse_date('20190702T211319Z'),
            datetime(2019, 7, 2, 21, 13, 19, tzinfo=UTC),
        )
        dt = parse_date('2019-07-02T21:13:19-04:00')
        self.assertEqual(
            dt.replace(tzinfo=None),
            datetime(2019, 7, 2, 21, 13, 19),
        )
        self.assertEqual(
            dt.astimezone(UTC),
            datetime(2019, 7, 3, 1, 13, 19, tzinfo=UTC),
        )
