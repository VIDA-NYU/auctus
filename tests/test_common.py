import unittest

from datamart_core import common


class TestDatasetIdEncoding(unittest.TestCase):
    def test_encode(self):
        """Test encoding a dataset ID to a file name."""
        self.assertEqual(
            common.encode_dataset_id('datamart_contrived/dataset#id;'),
            'datamart__contrived_2Fdataset_23id_3B',
        )

    def test_decode(self):
        """Test decoding a file name to a dataset ID."""
        self.assertEqual(
            common.decode_dataset_id('datamart__contrived_2Fdataset_23id_3B'),
            'datamart_contrived/dataset#id;',
        )
