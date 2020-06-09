import os
import tempfile


class SimpleConverterProxy(object):
    def __init__(self, writer, transform, name, temp_file, fp):
        self._writer = writer
        self._transform = transform
        self._name = name
        self._temp_file = temp_file
        self._fp = fp

    def close(self):
        self._fp.close()
        self._convert()

    def _convert(self):
        # Read back the file we wrote, and transform it to the final file
        with self._writer.open_file('w', self._name) as dst:
            self._transform(self._temp_file, dst)

    # Those methods forward to the actual file object

    def write(self, buffer):
        return self._fp.write(buffer)

    def flush(self):
        self._fp.flush()

    def __enter__(self):
        self._fp.__enter__()
        return self

    def __exit__(self, exc, value, tb):
        self._fp.__exit__(exc, value, tb)
        if exc is None:
            self._convert()


class SimpleConverter(object):
    """Base class for converters simply transforming files through a function.
    """
    def __init__(self, writer):
        self.writer = writer
        self.dir = tempfile.TemporaryDirectory(prefix='datamart_excel_')

    def set_metadata(self, dataset_id, metadata):
        self.writer.set_metadata(dataset_id, metadata)

    def open_file(self, mode='wb', name=None):
        temp_file = os.path.join(self.dir.name, 'file.xls')

        # Return a proxy that will write to the destination when closed
        if mode == 'wb':
            fp = open(temp_file, mode)
        elif mode == 'w':
            fp = open(temp_file, mode, encoding='utf-8', newline='')
        else:
            raise ValueError("Invalid write mode %r" % mode)
        return SimpleConverterProxy(
            self.writer, self.transform,
            name,
            temp_file, fp,
        )

    def finish(self):
        self.dir.cleanup()
        self.dir = None

    @staticmethod
    def transform(source_filename, dest_fileobj):
        raise NotImplementedError
