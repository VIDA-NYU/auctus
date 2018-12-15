import contextlib
import datamart_materialize
import logging
import os
import shutil
import tempfile

from .discovery import encode_dataset_id


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def get_dataset(materialize, dataset_id):
    shared = os.path.join('/datasets', encode_dataset_id(dataset_id))
    if os.path.exists(shared):
        yield shared
    else:
        temp_dir = tempfile.mkdtemp()
        try:
            temp_file = os.path.join(temp_dir, 'main.csv')
            datamart_materialize.download(materialize, temp_file, None)
            yield temp_dir
        finally:
            shutil.rmtree(temp_dir)
