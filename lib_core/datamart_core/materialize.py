import contextlib
import datamart_materialize
import os
import prometheus_client
import shutil
import tempfile

from .discovery import encode_dataset_id


PROM_DOWNLOAD = prometheus_client.Histogram(
    'download_seconds',
    "Materialization time",
    buckets=[1.0, 10.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
             float('inf')],
)


@contextlib.contextmanager
def get_dataset(metadata, dataset_id, format='csv'):
    shared = os.path.join('/datasets', encode_dataset_id(dataset_id))
    if os.path.exists(shared) and format == 'csv':
        # Read directly from stored file
        yield os.path.join(shared, 'main.csv')
    else:
        temp_dir = tempfile.mkdtemp()
        try:
            temp_file = os.path.join(temp_dir, 'data')
            if os.path.exists(shared):
                # Do format conversion from stored file
                with open(os.path.join(shared, 'main.csv'), 'rb') as src:
                    writer_cls = datamart_materialize.get_writer(format)
                    writer = writer_cls(dataset_id, temp_file, metadata)
                    with writer.open_file('wb') as dst:
                        shutil.copyfileobj(src, dst)
            else:
                # Materialize
                with PROM_DOWNLOAD.time():
                    datamart_materialize.download(
                        {'id': dataset_id, 'metadata': metadata},
                        temp_file, None, format=format)
            yield temp_file
        finally:
            shutil.rmtree(temp_dir)
