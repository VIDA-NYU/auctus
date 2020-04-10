import pandas
import unittest

from datamart_augmentation.augmentation import check_temporal_resolution


class TestTemporalResolutions(unittest.TestCase):
    def test_check(self):
        def get_res(data):
            idx = pandas.Index(pandas.to_datetime(data))
            return check_temporal_resolution(idx)

        self.assertEqual(
            get_res([
                '2020-01-14T21:05:02',
                '2020-01-14T21:05:07',
                '2020-01-14T21:05:30',
                '2020-01-14T22:21:54',
                '2020-01-15T05:12:41',
                '2020-03-21T05:12:41',
            ]),
            'second',
        )
        self.assertEqual(
            get_res([
                '2020-01-14T21:05:02',
                '2020-01-14T21:05:07',
                '2020-01-14T21:05:30',
            ]),
            'second',
        )
        self.assertEqual(
            get_res([
                '2020-01-14T21:05:00Z',
                '2020-01-14T21:05:00Z',
                '2020-01-14T21:05:00Z',
                '2020-01-14T22:21:00Z',
                '2020-01-15T05:12:00Z',
                '2020-03-21T05:12:00Z',
            ]),
            'minute',
        )
        self.assertEqual(
            get_res([
                '2020-01-14T21:30:00',
                '2020-01-14T21:30:00',
                '2020-01-14T21:30:00',
                '2020-01-14T22:30:00',
                '2020-01-15T05:30:00',
                '2020-03-21T05:30:00',
            ]),
            'hour',
        )
        self.assertEqual(
            get_res([
                '2020-01-14',
                '2020-01-14',
                '2020-01-14',
                '2020-01-14',
                '2020-01-15',
                '2020-03-21',
            ]),
            'day',
        )
        self.assertEqual(
            get_res([
                '2020-01-14T05:12:41',
                '2020-01-14T05:12:41',
                '2020-01-14T05:12:41',
                '2020-01-14T05:12:41',
                '2020-01-15T05:12:41',
                '2020-03-21T05:12:41',
            ]),
            'day',
        )
        self.assertEqual(
            get_res([
                '2020-02-06',
                '2020-02-06',
                '2020-02-13',
                '2020-02-27',
                '2020-03-05',
            ]),
            'week',
        )
        self.assertEqual(
            get_res([
                '2020-02-06',
                '2020-02-06',
                '2020-03-06',
                '2020-05-06',
                '2020-06-06',
            ]),
            'month',
        )
        self.assertEqual(
            get_res([
                '2017-02-06',
                '2018-02-06',
                '2018-02-06',
                '2020-02-06',
            ]),
            'year',
        )
