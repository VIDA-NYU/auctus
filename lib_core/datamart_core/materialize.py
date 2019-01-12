import contextlib
import datamart_materialize
import logging
import os
import prometheus_client
import shutil
import tempfile

from .discovery import encode_dataset_id


logger = logging.getLogger(__name__)



PROM_DOWNLOAD = prometheus_client.Histogram('download_seconds',
                                            "Materialization time")


@contextlib.contextmanager
def get_dataset(metadata, dataset_id, format='csv'):
    shared = os.path.join('/datasets', encode_dataset_id(dataset_id))
    if os.path.exists(shared):
        yield os.path.join(shared, 'main.csv')
    else:
        temp_dir = tempfile.mkdtemp()
        try:
            temp_file = os.path.join(temp_dir, 'data')
            with PROM_DOWNLOAD.time():
                datamart_materialize.download(
                    {'id': dataset_id, 'metadata': metadata},
                    temp_file, None, format=format)
            yield temp_file
        finally:
            shutil.rmtree(temp_dir)
