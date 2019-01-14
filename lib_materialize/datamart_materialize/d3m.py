import json
import os


STRUCTURAL_TYPE_MAP = {
    'http://schema.org/Boolean': 'boolean',
    'http://schema.org/Integer': 'integer',
    'http://schema.org/Float': 'real',
    'http://schema.org/Text': 'string',
}


class D3mWriter(object):
    needs_metadata = True

    def __init__(self, dataset_id, destination, metadata):
        self.destination = destination
        os.mkdir(destination)
        os.mkdir(os.path.join(destination, 'tables'))

        columns = []
        for i, column in enumerate(metadata['columns']):
            if 'http://schema.org/DateTime' in column['semantic_types']:
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
                'datasetSchemaVersion': '3.2.0',
                'redacted': False,
                'datasetVersion': '0.0',
            },
            'dataResources': [
                {
                    'resID': '0',
                    'resPath': 'tables/learningData.csv',
                    'resType': 'table',
                    'resFormat': ['text/csv'],
                    'isCollection': False,
                    'columns': columns,
                },
            ],
        }

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
