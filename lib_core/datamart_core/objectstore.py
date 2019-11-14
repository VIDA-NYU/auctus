import asyncio
import boto3
from botocore.client import Config
import io
import logging
import os
import weakref


logger = logging.getLogger(__name__)


def get_object_store():
    return ObjectStore(
        os.environ['S3_URL'],
        os.environ['S3_CLIENT_URL'],
        os.environ['S3_BUCKET_PREFIX'],
    )


class NoSuchObject(KeyError):
    pass


class StreamUpload(object):
    def __init__(self, s3_core, bucket, objectname):
        self.s3_core = s3_core
        self.bucket = bucket
        self.objectname = objectname

        res = self.s3_core.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.objectname,
        )
        self.upload_id = res['UploadId']
        self.partnumber = 1
        self.parts = []

        # "Each part must be at least 5 MB in size, except the last part"
        self.current_data = io.BytesIO()

        self._finalizer = weakref.finalize(self, self.abort)

    def _write_part(self):
        self.current_data.seek(0, 0)
        res = self.s3_core.upload_part(
            Bucket=self.bucket,
            Key=self.objectname,
            UploadId=self.upload_id,
            Body=self.current_data,
            PartNumber=self.partnumber,
        )
        self.parts.append({'PartNumber': self.partnumber, 'ETag': res['ETag']})
        self.partnumber += 1
        self.current_data.seek(0, 0)
        self.current_data.truncate()

    def write(self, bytes):
        self.current_data.write(bytes)

        if self.current_data.tell() >= 5242880:
            self._write_part()

    def complete(self):
        if self.current_data.tell() > 0:
            self._write_part()

        self.s3_core.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.objectname,
            UploadId=self.upload_id,
            MultipartUpload={
                'Parts': self.parts,
            },
        )

    def abort(self):
        self.s3_core.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.objectname,
            UploadId=self.upload_id,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.abort()
        else:
            self.complete()


class ObjectStore(object):
    # FIXME: boto has 3 different interfaces for everything, this uses a mix
    # (Bucket/Object objects, client directly, low-level client.meta.client)

    def __init__(self, endpoint_url, client_endpoint_url, bucket_prefix):
        self.s3 = boto3.resource(
            's3', endpoint_url=endpoint_url,
            aws_access_key_id=os.environ['S3_KEY'],
            aws_secret_access_key=os.environ['S3_SECRET'],
            config=Config(signature_version='s3v4'),
        )
        self.s3_client = boto3.resource(
            's3', endpoint_url=client_endpoint_url,
            aws_access_key_id=os.environ['S3_KEY'],
            aws_secret_access_key=os.environ['S3_SECRET'],
            config=Config(signature_version='s3v4'),
        )
        self.bucket_prefix = bucket_prefix

    def bucket_name(self, name):
        if name not in ('datasets', 'augmentations'):
            raise ValueError("Invalid bucket name %s" % name)

        name = '%s%s' % (self.bucket_prefix, name)
        return name

    def bucket(self, name):
        return self.s3.Bucket(self.bucket_name(name))

    def list_bucket(self, bucket):
        return (
            {'name': obj.key, 'size': obj.size}
            for obj in self.bucket(bucket).objects.all()
        )

    def get(self, bucket, objectname):
        try:
            res = self.s3.meta.client.head_object(
                Bucket=self.bucket_name(bucket),
                Key=objectname,
            )
            return {'key': objectname, 'size': res['ContentLength']}
        except self.s3.meta.client.exceptions.NoSuchKey:
            raise NoSuchObject

    def download_file(self, bucket, objectname, filename):
        try:
            self.bucket(bucket).download_file(objectname, filename)
        except self.s3.meta.client.exceptions.NoSuchKey:
            raise NoSuchObject

    def download_fileobj(self, bucket, objectname):
        try:
            obj = self.s3.Object(self.bucket_name(bucket), objectname)
            return obj.get()['Body']
        except self.s3.meta.client.exceptions.NoSuchKey:
            raise NoSuchObject

    def download_bytes(self, bucket, objectname):
        try:
            obj = self.s3.Object(self.bucket_name(bucket), objectname)
            return obj.get()['Body'].read()
        except self.s3.meta.client.exceptions.NoSuchKey:
            raise NoSuchObject

    def upload_fileobj(self, bucket, objectname, fileobj):
        self.s3.Object(self.bucket_name(bucket), objectname).put(Body=fileobj)

    def upload_file(self, bucket, objectname, filename):
        self.s3.meta.client.upload_file(filename,
                                        self.bucket_name(bucket), objectname)

    def upload_file_async(self, bucket, objectname, filename):
        return asyncio.get_event_loop().run_in_executor(
            None,
            self.upload_file,
            bucket, objectname, filename,
        )

    def upload_bytes(self, bucket, objectname, bytestr):
        self.upload_fileobj(bucket, objectname, io.BytesIO(bytestr))

    def multipart_upload(self, bucket, objectname):
        return StreamUpload(self.s3.meta.client,
                            self.bucket_name(bucket), objectname)

    def upload_bytes_async(self, bucket, objectname, bytestr):
        return asyncio.get_event_loop().run_in_executor(
            None,
            self.upload_bytes,
            bucket, objectname, bytestr,
        )

    def delete(self, bucket, objectname):
        self.s3.Object(self.bucket_name(bucket), objectname).delete()

    def create_buckets(self):
        buckets = set(bucket.name for bucket in self.s3.buckets.all())
        missing = []
        for name in ('datasets', 'augmentations'):
            name = self.bucket_name(name)
            if name not in buckets:
                missing.append(name)

        if missing:
            logger.info("The buckets don't seem to exist; creating %s",
                        ", ".join(missing))
            for name in missing:
                self.s3.create_bucket(Bucket=name)

    def clear_bucket(self, bucket):
        keys = []
        for obj in self.bucket(bucket).objects.all():
            keys.append(obj.key)
            if len(keys) >= 990:
                self.s3.meta.client.delete_objects(
                    Bucket=self.bucket_name(bucket),
                    Delete={
                        'Objects': [{'Key': key} for key in keys],
                    },
                )
                keys = []
        if keys:
            self.s3.meta.client.delete_objects(
                Bucket=self.bucket_name(bucket),
                Delete={
                    'Objects': [{'Key': key} for key in keys],
                },
            )

    def presigned_serve_url(self, bucket, objectname, filename, mime=None):
        return self.s3_client.meta.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': self.bucket_name(bucket),
                    'Key': objectname,
                    'ResponseContentType': mime or 'application/octet-stream',
                    'ResponseContentDisposition': 'inline; filename=%s' %
                                                  filename},
        )
