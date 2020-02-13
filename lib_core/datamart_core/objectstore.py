from base64 import b64decode
import contextlib
import json
import logging
import os
from urllib.parse import ParseResult, urlparse, urlunparse


logger = logging.getLogger(__name__)


def get_object_store():
    if os.environ.get('S3_KEY') and os.environ.get('GCS_CREDS'):
        raise EnvironmentError
    elif os.environ.get('S3_KEY'):
        return S3ObjectStore()
    elif os.environ.get('GCS_CREDS'):
        return GCSObjectStore()
    else:
        raise EnvironmentError


class ObjectStore(object):
    BUCKETS = (
        'datasets',
        'cached-datasets', 'cached-augmentations', 'cached-profiles',
    )

    def __init__(self, fs, prefix=None):
        self.fs = fs
        self.prefix = prefix
        for bucket in self.BUCKETS:
            try:
                self.fs.mkdir(self.bucket(bucket))
            except Exception:
                pass

    def bucket(self, name):
        assert name in self.BUCKETS
        return '%s%s' % (self.prefix, name)

    def open(self, bucket, name, mode='rb'):
        full_name = '%s/%s' % (self.bucket(bucket), name)
        if 'w' in mode:
            # Manually commit if __exit__ without error
            fp = self.fs.open(full_name, mode, autocommit=False)
            return _commit_discard_context(fp, full_name)
        else:
            return self.fs.open(full_name, mode)

    def delete(self, bucket, name):
        self.fs.rm(
            '%s/%s' % (self.bucket(bucket), name),
            recursive=False,
        )

    def delete_prefix(self, bucket, name_prefix):
        prefix = '%s/%s' % (self.bucket(bucket), name_prefix)
        assert not any(char in prefix for char in '[]*?')
        files = self.fs.glob(prefix + '*')
        self.fs.rm(files, recursive=False)

    def clear_bucket(self, bucket):
        bucket = self.bucket(bucket)
        files = self.fs.ls(bucket, detail=False)
        for filename in files:
            self.fs.rm(filename, recursive=False)

    @staticmethod
    def _remove_bucket_prefix(name):
        idx = name.find('/', 1)
        return name[idx + 1:]

    def list_bucket_details(self, bucket):
        bucket = self.bucket(bucket)

        return [
            dict(item, name=self._remove_bucket_prefix(item['name']))
            for item in self.fs.ls(bucket, detail=True)
        ]

    def list_bucket_names(self, bucket):
        bucket = self.bucket(bucket)

        return [
            self._remove_bucket_prefix(name)
            for name in self.fs.ls(bucket, detail=False)
        ]

    def _build_client_url(self, url):
        return url

    def url(self, bucket, name):
        url = self.fs.url(self.bucket(bucket) + '/' + name)
        return self._build_client_url(url)

    def file_url(self, fileobj):
        return self._build_client_url(fileobj.url())


class _ObjecStoreFileWrapper(object):
    def __init__(self, fp):
        self._fileobj = fp

    def write(self, buf):
        return self._fileobj.write(buf)

    def flush(self):
        self._fileobj.flush()

    def close(self):
        raise TypeError("Attempted to close ObjecStoreFileWrapper")

    def __enter__(self):
        raise TypeError("Attempted to enter ObjecStoreFileWrapper")

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise TypeError("Attempted to exit ObjecStoreFileWrapper")

    def __iter__(self):
        raise TypeError("Attempted to iter ObjecStoreFileWrapper")


@contextlib.contextmanager
def _commit_discard_context(fp, filename):
    try:
        with fp:
            logger.info("opened file %s", filename)
            yield _ObjecStoreFileWrapper(fp)
    except:
        logger.info("exception, discarding file %s", filename)
        fp.discard()
        raise
    else:
        logger.info("committing file %s", filename)
        fp.commit()


class GCSObjectStore(ObjectStore):
    def __init__(self):
        import gcsfs

        fs = gcsfs.GCSFileSystem(
            project=os.environ['GCS_PROJECT'],
            token=json.loads(b64decode(os.environ['GCS_CREDS'])),
        )
        prefix = os.environ['GCS_BUCKET_PREFIX']

        super(GCSObjectStore, self).__init__(fs, prefix)

    def clear_bucket(self, bucket):
        bucket = self.bucket(bucket)
        files = self.fs.ls(bucket, detail=False)
        self.fs.rm(files, recursive=False)


class S3ObjectStore(ObjectStore):
    def __init__(self):
        import s3fs

        client_kwargs = {}
        if 'S3_URL' in os.environ:
            client_kwargs['endpoint_url'] = os.environ['S3_URL']
        fs = s3fs.S3FileSystem(
            key=os.environ['S3_KEY'],
            secret=os.environ['S3_SECRET'],
            client_kwargs=client_kwargs,
        )
        prefix = os.environ['S3_BUCKET_PREFIX']
        self.client_url = urlparse(os.environ['S3_CLIENT_URL'])

        super(S3ObjectStore, self).__init__(fs, prefix)

    def clear_bucket(self, bucket):
        bucket = self.bucket(bucket)
        files = self.fs.ls(bucket, detail=False)
        self.fs.bulk_delete(files, recursive=False)

    def _build_client_url(self, url):
        parsed = urlparse(url)
        return urlunparse(ParseResult(
            self.client_url.scheme, self.client_url.netloc,
            parsed.path, parsed.params, parsed.query, parsed.fragment,
        ))
