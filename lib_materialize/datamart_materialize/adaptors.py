import codecs
from fsspec.implementations.local import LocalFileSystem
import os
import zipfile


class FsWriter(object):
    """Backend writer putting files in a directory.
    """
    def __init__(self, destination, fs=None):
        if fs is None:
            self.fs = LocalFileSystem()
        else:
            self.fs = fs
        self.destination = destination

    def open_file(self, mode='wb', name=None, newline='', **kwargs):
        if name is None:
            return self.fs.open(self.destination, mode, **kwargs)
        else:
            dirname = os.path.dirname(name)
            if dirname:
                self.fs.makedirs(dirname, exist_ok=True)
            return self.fs.open(
                self.destination + '/' + name,
                mode,
                newline=newline,
                **kwargs,
            )

    def set_metadata(self, dataset_id, metadata):
        pass

    def finish(self):
        return None


class ZipWriter(object):
    """Backend writer putting files in a ZIP.
    """
    def __init__(self, zip_file):
        self.zip = zipfile.ZipFile(zip_file, 'w')

    def open_file(self, mode='wb', name=None, encoding='utf-8'):
        if name is None:
            name = 'data.csv'
        fp = self.zip.open(name, mode='w')
        if mode == 'wb':
            return fp
        else:
            return codecs.getwriter(encoding)(fp)

    def set_metadata(self, dataset_id, metadata):
        pass

    def finish(self):
        self.zip.close()
