import codecs
from datetime import datetime
import io
import json
import logging
import tarfile

from datamart_core.common import PrefixedElasticsearch, encode_dataset_id, \
    setup_logging
from datamart_core.objectstore import get_object_store


logger = logging.getLogger(__name__)


SIZE = 10000


_unique_filenames = {}


def unique_filename(pattern):
    """Return a file name with an incrementing number to make it unique.
    """
    number = _unique_filenames.get(pattern, 0) + 1
    _unique_filenames[pattern] = number
    return pattern.format(number)


def dump_json_to_tar(tar, obj, name):
    binary_file = io.BytesIO()
    text_file = codecs.getwriter('utf-8')(binary_file)
    json.dump(obj, text_file, sort_keys=True, indent=2)
    tar_info = tarfile.TarInfo(name)
    tar_info.size = binary_file.tell()
    binary_file.seek(0, 0)
    tar.addfile(
        tar_info,
        binary_file,
    )


def snapshot():
    es = PrefixedElasticsearch()
    object_store = get_object_store()

    tarname = '%s.tar.gz' % datetime.utcnow().strftime('%Y-%m-%d')
    with object_store.open('snapshots', tarname, 'wb') as fp:
        with tarfile.open(tarname, 'w:gz', fileobj=fp) as tar:
            logger.info("Dumping datasets")
            hits = es.scan(
                index='datasets',
                query={
                    'query': {
                        'match_all': {},
                    },
                },
                size=SIZE,
            )
            for h in hits:
                # Use dataset ID as file name
                filename = encode_dataset_id(h['_id'])

                dump_json_to_tar(tar, h['_source'], filename)

            logger.info("Dumping Lazo data")
            hits = es.scan(
                index='lazo',
                query={
                    'query': {
                        'match_all': {},
                    },
                },
                size=SIZE,
            )
            for h in hits:
                # Use "lazo." dataset_id ".NB" as file name
                dataset_id = h['_id'].split('__.__')[0]
                filename = unique_filename(
                    'lazo.{0}.{{0}}'.format(encode_dataset_id(dataset_id))
                )
                dump_json_to_tar(
                    tar,
                    dict(h['_source'], _id=h['_id']),
                    filename,
                )


def main():
    setup_logging()

    snapshot()
