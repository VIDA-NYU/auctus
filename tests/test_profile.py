from datetime import datetime
from dateutil.tz import UTC
import unittest
from unittest.mock import call, patch

from datamart_profiler import pair_latlong_columns, \
    normalize_latlong_column_name
from datamart_profiler import profile_types


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
            profile_types.parse_date('Monday July 1, 2019'),
            datetime(2019, 7, 1, tzinfo=UTC),
        )
        self.assertEqual(
            profile_types.parse_date('20190702T211319Z'),
            datetime(2019, 7, 2, 21, 13, 19, tzinfo=UTC),
        )
        dt = profile_types.parse_date('2019-07-02T21:13:19-04:00')
        self.assertEqual(
            dt.replace(tzinfo=None),
            datetime(2019, 7, 2, 21, 13, 19),
        )
        self.assertEqual(
            dt.astimezone(UTC),
            datetime(2019, 7, 3, 1, 13, 19, tzinfo=UTC),
        )


class TestTypes(unittest.TestCase):
    def do_test(self, match, positive, negative):
        for elem in positive.splitlines():
            elem = elem.strip()
            if elem:
                self.assertTrue(match(elem),
                                "Didn't match: %s" % elem)
        for elem in negative.splitlines():
            elem = elem.strip()
            if elem:
                self.assertFalse(match(elem),
                                 "Shouldn't have matched: %s" % elem)

    def test_phone(self):
        positive = '''\
        +1 347 123 4567
        1 347 123 4567
        13471234567
        +13471234567
        +1 (347) 123 4567
        (347)123-4567
        +1.347-123-4567
        347-123-4567
        +33 6 12 34 56 78
        06 12 34 56 78
        +1.347123456
        347.123.4567
        '''
        negative = '''\
        -3471234567
        12.3
        +145
        -
        '''
        self.do_test(
            profile_types._re_phone.match,
            positive, negative,
        )
        self.assertFalse(profile_types._re_phone.match(''))

    def test_ints(self):
        positive = '''\
        12
        0
        +478
        -17
        '''
        negative = '''\
        1.7
        7A
        ++2
        --34
        +-7
        -+18
        '''
        self.do_test(
            profile_types._re_int.match,
            positive, negative,
        )
        self.assertFalse(profile_types._re_int.match(''))

    def test_floats(self):
        positive = '''\
        12.
        0.
        .7
        123.456
        +123.456
        +.456
        -.4
        .4e17
        -.4e17
        +8.4e17
        +8.e17
        '''
        negative = '''\
        1.7.3
        .7.3
        7.3.
        .
        -.
        +.
        7.A
        .e8
        8e17
        1.3e
        '''
        self.do_test(
            profile_types._re_float.match,
            positive, negative,
        )
        self.assertFalse(profile_types._re_float.match(''))


class TestTruncate(unittest.TestCase):
    def test_simple(self):
        from datamart_profiler import truncate_string

        self.assertEqual(truncate_string("abc", 10), "abc")
        self.assertEqual(truncate_string("abcdefghij", 10), "abcdefghij")
        self.assertEqual(truncate_string("abcdefghijk", 10), "abcdefg...")
        self.assertEqual(truncate_string("abcdefghijklmnop", 10), "abcdefg...")

    def test_words(self):
        from datamart_profiler import truncate_string

        self.assertEqual(
            truncate_string("abcde fghijklmnopqrs tuvwxyzABCD EF", 30),
            "abcde fghijklmnopqrs...",
        )
        self.assertEqual(
            truncate_string("abcde fghijklmnopqrs tu vwxyzABCD EF", 30),
            "abcde fghijklmnopqrs tu...",
        )
        self.assertEqual(
            truncate_string("abc defghijklmnopqrstuvwxyzABCDEFGHI", 30),
            "abc defghijklmnopqrstuvwxyz...",
        )
