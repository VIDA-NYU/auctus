import copy
import json
import os
import shutil
import tempfile
import unittest

from datamart_materialize.d3m import D3mWriter

from .utils import data


basic_metadata = {
    'name': 'basic',
    "size": 425,
    'columns': [
        {
            "name": "name",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [],
        },
        {
            "name": "country",
            "structural_type": "http://schema.org/Text",
            "semantic_types": ["http://schema.org/Enumeration"],
        },
        {
            "name": "number",
            "structural_type": "http://schema.org/Integer",
            "semantic_types": [],
        },
        {
            "name": "what",
            "structural_type": "http://schema.org/Text",
            "semantic_types": [
                "http://schema.org/Boolean",
                "http://schema.org/Enumeration"
            ],
        }
    ],
}


basic_d3m_metadata = {
    'about': {
        'datasetID': 'test1',
        'datasetName': 'basic',
        'datasetSchemaVersion': '4.0.0',
        'datasetVersion': '1.0',
        'approximateSize': '425 B',
        'license': 'unknown',
        'redacted': False,
    },
    'dataResources': [
        {
            'resID': 'learningData',
            'resType': 'table',
            'resFormat': {'text/csv': ['csv']},
            'resPath': 'tables/learningData.csv',
            'isCollection': False,
            'columns': [
                {
                    'colIndex': 0,
                    'colName': 'name',
                    'colType': 'string',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 1,
                    'colName': 'country',
                    'colType': 'categorical',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 2,
                    'colName': 'number',
                    'colType': 'integer',
                    'role': ['attribute'],
                },
                {
                    'colIndex': 3,
                    'colName': 'what',
                    'colType': 'boolean',
                    'role': ['attribute'],
                },
            ],
        },
    ],
}


basic_d3m_metadata_with_index = copy.deepcopy(basic_d3m_metadata)
basic_d3m_metadata_with_index['dataResources'][0]['columns'] = (
        [{
            'colIndex': 0,
            'colName': 'd3mIndex',
            'colType': 'integer',
            'role': ['index'],
        }] +
        [
            dict(col, colIndex=col['colIndex'] + 1)
            for col in basic_d3m_metadata['dataResources'][0]['columns']
        ]
)


class TestD3m(unittest.TestCase):
    def _check_output(self, target, *,
                      metadata=basic_d3m_metadata, data_path='basic.csv'):
        with open(
            os.path.join(target, 'tables', 'learningData.csv'), 'rb'
        ) as fp:
            with data(data_path) as f_ref:
                self.assertEqual(
                    fp.read(),
                    f_ref.read(),
                )
        with open(
            os.path.join(target, 'datasetDoc.json'), 'r'
        ) as fp:
            self.assertEqual(
                json.load(fp),
                metadata,
            )

    def test_writer_default(self):
        """Test writing with default parameters."""
        with tempfile.TemporaryDirectory() as temp:
            target = os.path.join(temp, 'dataset')
            writer = D3mWriter('test1', target, basic_metadata)
            with data('basic.csv') as f_in, writer.open_file() as f_out:
                shutil.copyfileobj(f_in, f_out)
            writer.finish()

            self._check_output(target)

    def test_d3m_writer_4(self):
        """Test writing in 4.0.0 format explicitely."""
        with tempfile.TemporaryDirectory() as temp:
            target = os.path.join(temp, 'dataset')
            writer = D3mWriter('test1', target, basic_metadata,
                               format_options={'version': '4.0.0'})
            with data('basic.csv') as f_in, writer.open_file() as f_out:
                shutil.copyfileobj(f_in, f_out)
            writer.finish()

            self._check_output(target)

    def test_writer_32(self):
        """Test writing in 3.2.0 format."""
        with tempfile.TemporaryDirectory() as temp:
            target = os.path.join(temp, 'dataset')
            writer = D3mWriter('test1', target, basic_metadata,
                               format_options={'version': '3.2.0'})
            with data('basic.csv') as f_in, writer.open_file() as f_out:
                shutil.copyfileobj(f_in, f_out)
            writer.finish()

            meta = copy.deepcopy(basic_d3m_metadata)
            meta['about']['datasetSchemaVersion'] = '3.2.0'
            meta['dataResources'][0]['resFormat'] = ['text/csv']
            self._check_output(target, metadata=meta)

    def test_index_add(self):
        """Test adding the index to data that doesn't have it."""
        with tempfile.TemporaryDirectory() as temp:
            target = os.path.join(temp, 'dataset')
            writer = D3mWriter('test1', target, basic_metadata,
                               format_options={'need_d3mindex': True})
            with data('basic.csv') as f_in, writer.open_file() as f_out:
                shutil.copyfileobj(f_in, f_out)
            writer.finish()

            self._check_output(
                target,
                metadata=basic_d3m_metadata_with_index,
                data_path='basic.d3m.csv',
            )

    def test_index_present(self):
        """Test that requiring an index doesn't add one if already there."""
        with tempfile.TemporaryDirectory() as temp:
            target = os.path.join(temp, 'dataset')
            writer = D3mWriter('test1', target, basic_metadata,
                               format_options={'need_d3mindex': True})
            with data('basic.d3m.csv') as f_in, writer.open_file() as f_out:
                shutil.copyfileobj(f_in, f_out)
            writer.finish()

            self._check_output(
                target,
                metadata=basic_d3m_metadata_with_index,
                data_path='basic.d3m.csv',
            )
