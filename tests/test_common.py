import unittest

from datamart_core import common


class TestDatasetIdEncoding(unittest.TestCase):
    def test_encode(self):
        """Test encoding a dataset ID to a file name"""
        self.assertEqual(
            common.encode_dataset_id('datamart_contrived/dataset#id;'),
            'datamart__contrived_2Fdataset_23id_3B',
        )

    def test_decode(self):
        """Test decoding a file name to a dataset ID"""
        self.assertEqual(
            common.decode_dataset_id('datamart__contrived_2Fdataset_23id_3B'),
            'datamart_contrived/dataset#id;',
        )


class TestStripHtml(unittest.TestCase):
    def test_strip(self):
        """Strip HTML from text"""
        self.assertEqual(
            common.strip_html(
                "<p>Text & <em>tags</em> &amp; <acronym title=\"HyperText "
                + "Markup Language\">HTML</acronym></p>",
            ),
            "Text & tags & HTML",
        )

    def test_link(self):
        """Keep link targets"""
        self.assertEqual(
            common.strip_html(
                "Some <a href=\"https://google.com/\">links</a> here: "
                + "<a href=\"https://google.com/\">google.com</a>",
            ),
            "Some links (https://google.com/) here: google.com",
        )

    def test_unknown(self):
        """Unknown tags should be preserved"""
        self.assertEqual(
            common.strip_html(
                "Run python <program>",
            ),
            "Run python <program>",
        )
