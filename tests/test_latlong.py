import unittest
from unittest.mock import call, patch

from datamart_profiler import pair_latlon_columns, normalize_latlon_column_name


class TestLatlongSelection(unittest.TestCase):
    def test_normalize_name(self):
        """Test normalizing column names."""
        self.assertEqual(
            normalize_latlon_column_name('latitude', 'latitude', 'lat'),
            '',
        )
        self.assertEqual(
            normalize_latlon_column_name('Pickup_Latitude', 'latitude', 'lat'),
            'pickup_',
        )
        self.assertEqual(
            normalize_latlon_column_name('start_Lat_deg', 'latitude', 'lat'),
            'start__deg',
        )

    def test_pairing(self):
        """Test pairing latitude and longitude columns by name."""
        with patch('datamart_profiler.logger') as mock_warn:
            pairs = pair_latlon_columns(
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
