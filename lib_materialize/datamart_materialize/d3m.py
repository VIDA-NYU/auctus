import json
import os

from . import types


STRUCTURAL_TYPE_MAP = {
    types.BOOLEAN: 'boolean',
    types.INTEGER: 'integer',
    types.FLOAT: 'real',
    types.TEXT: 'string',
}


DEFAULT_VERSION = '4.0.0'


def d3m_metadata(dataset_id, metadata, *, version=None):
    if not version:
        version = DEFAULT_VERSION
    elif version not in ('3.2.0', '4.0.0'):
        raise ValueError("Unknown D3M schema version %r" % (version,))

    columns = []
    for i, column in enumerate(metadata['columns']):
        # D3M has a 'dateTime' structural type but we use string
        if types.DATE_TIME in column['semantic_types']:
            col_type = 'dateTime'
        else:
            if types.BOOLEAN in column['semantic_types']:
                col_type = 'boolean'
            elif types.CATEGORICAL in column['semantic_types']:
                col_type = 'categorical'
            elif types.DATE_TIME in column['semantic_types']:
                col_type = 'dateTime'
            else:
                col_type = STRUCTURAL_TYPE_MAP.get(
                    column['structural_type'],
                    'string',
                )
        role = 'index' if column['name'] == 'd3mIndex' else 'attribute'
        columns.append({
            'colIndex': i,
            'colName': column['name'],
            'colType': col_type,
            'role': [role],
        })

    d3m_meta = {
        'about': {
            'datasetID': dataset_id,
            'datasetName': metadata.get('name', dataset_id),
            'license': metadata.get('license', 'unknown'),
            'approximateSize': '%d B' % metadata['size'],
            'datasetSchemaVersion': version,
            'redacted': False,
            'datasetVersion': '1.0',
        },
        'dataResources': [
            {
                'resID': 'learningData',
                'resPath': 'tables/learningData.csv',
                'resType': 'table',
                'resFormat': (
                    {'text/csv': ["csv"]} if version == '4.0.0'
                    else ['text/csv']
                ),
                'isCollection': False,
                'columns': columns,
            },
        ],
    }
    if 'description' in metadata:
        d3m_meta['about']['description'] = metadata['description']
    if 'qualities' in metadata:
        d3m_meta['qualities'] = metadata.get('qualities')

    return d3m_meta


class D3mWriter(object):
    """Writer for MIT-LL's schema for D3M datasets.

    https://gitlab.com/datadrivendiscovery/data-supply
    """
    needs_metadata = True
    default_options = {'version': DEFAULT_VERSION}

    @classmethod
    def _get_opt(cls, options, key):
        if options and key in options:
            return options.pop(key)
        else:
            return cls.default_options[key]

    def __init__(self, dataset_id, destination, metadata, format_options=None):
        version = self._get_opt(format_options, 'version')
        if format_options:
            raise ValueError(
                "Invalid format option %r" % (next(iter(format_options)),)
            )

        self.destination = destination
        os.mkdir(destination)
        os.mkdir(os.path.join(destination, 'tables'))

        d3m_meta = d3m_metadata(dataset_id, metadata, version=version)

        with open(os.path.join(destination, 'datasetDoc.json'), 'w') as fp:
            json.dump(d3m_meta, fp, sort_keys=True, indent=2)

    def open_file(self, mode='wb', name=None, **kwargs):
        if name is not None:
            raise ValueError("D3mWriter can only write single-table datasets "
                             "for now")
        return open(os.path.join(self.destination,
                                 'tables', 'learningData.csv'),
                    mode, **kwargs)

    def finish(self):
        return None
