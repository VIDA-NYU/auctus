import csv
import json
import logging
import os

from . import types


logger = logging.getLogger(__name__)


STRUCTURAL_TYPE_MAP = {
    types.BOOLEAN: 'boolean',
    types.INTEGER: 'integer',
    types.FLOAT: 'real',
    types.TEXT: 'string',
}


DEFAULT_VERSION = '4.0.0'


def d3m_metadata(dataset_id, metadata, *, version=None, need_d3mindex=False):
    return {}


class D3mWriter(object):
    """Writer for the ``d3m`` dataset format, following MIT-LL's schema.

    https://gitlab.com/datadrivendiscovery/data-supply

    The key ``version`` can be passed in `format_options` to select the version
    of the schema to generate, between ``3.2.0`` and ``4.0.0``.
    """
    needs_metadata = True
    default_options = {'version': DEFAULT_VERSION, 'need_d3mindex': False}

    @classmethod
    def _get_opt(cls, options, key):
        if options and key in options:
            return options.pop(key)
        else:
            return cls.default_options[key]

    def __init__(self, dataset_id, destination, metadata, format_options=None):
        version = self._get_opt(format_options, 'version')
        self.need_d3mindex = self._get_opt(format_options, 'need_d3mindex')
        if format_options:
            raise ValueError(
                "Invalid format option %r" % (next(iter(format_options)),)
            )

        self.destination = destination
        os.mkdir(destination)
        os.mkdir(os.path.join(destination, 'tables'))

        d3m_meta = d3m_metadata(
            dataset_id, metadata,
            version=version, need_d3mindex=self.need_d3mindex,
        )

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
        if self.need_d3mindex:
            overwrite = False
            tables = os.path.join(self.destination, 'tables')
            data_path = os.path.join(tables, 'learningData.csv')
            tmp_path = os.path.join(tables, 'learningData.index.csv')
            with open(data_path) as f_in:
                reader = iter(csv.reader(f_in))

                try:
                    columns = next(reader)
                except StopIteration:
                    return None
                if 'd3mIndex' not in columns:
                    logger.info("No 'd3mIndex' column, generating one")
                    overwrite = True
                    with open(
                        tmp_path,
                        'w',
                    ) as f_out:
                        writer = csv.writer(f_out)
                        writer.writerow(['d3mIndex'] + columns)
                        for idx, row in enumerate(reader):
                            writer.writerow([idx] + row)
            if overwrite:
                os.rename(tmp_path, data_path)

        return None
