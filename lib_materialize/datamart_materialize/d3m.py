import codecs
import csv
import io
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


class _D3mAddIndex(object):
    BUFFER_MAX = 102400  # 100 kiB

    def __init__(self, dest_fp, binary):
        self._buffer = io.StringIO()
        self._generate = None
        self._dest_fp = dest_fp
        self._dest_csv = None
        self._idx = -1
        if binary:
            self._decoder = codecs.getincrementaldecoder('utf-8')()
        else:
            self._decoder = None

    def __iter__(self):
        # Pandas needs file objects to have __iter__
        return self

    def _peek_line(self):
        # Make a CSV reader for the buffer
        self._buffer.seek(0, 0)
        reader = iter(csv.reader(self._buffer))

        # Try to read two lines, which means we have one complete one
        try:
            line = next(reader)
            next(reader)
        except StopIteration:
            line = None

        # Reset buffer
        self._buffer.seek(0, 2)

        return line

    def _get_lines(self):
        # Make a CSV reader for the buffer
        self._buffer.seek(0, 0)
        reader = iter(csv.reader(self._buffer))

        # Read lines, making sure they're complete by reading the following one
        try:
            prev = next(reader)
        except StopIteration:
            return
        prevpos = 0
        pos = self._buffer.tell()
        while prev:
            try:
                line = next(reader)
            except StopIteration:
                break
            yield prev
            prev = line
            prevpos = pos
            pos = self._buffer.tell()

        # Remove what was read from the buffer
        self._buffer = io.StringIO(self._buffer.getvalue()[prevpos:])
        self._buffer.seek(0, 2)

    def write(self, buf):
        if self._decoder is not None:
            buf = self._decoder.decode(buf)
        if self._generate is False:
            return self._dest_fp.write(buf)
        self._buffer.write(buf)
        if self._buffer.tell() > self.BUFFER_MAX:
            self._flush()
        return len(buf)

    def _flush(self):
        if self._generate is None:
            # Decide whether the index needs to be generated
            columns = self._peek_line()
            if columns is None:
                raise ValueError("Couldn't read CSV header")
            if 'd3mIndex' in columns:
                self._generate = False
                self._dest_fp.write(self._buffer.getvalue())
                self._buffer = None
                return
            else:
                logger.info("No 'd3mIndex' column, generating one")
                self._generate = True
                self._dest_csv = csv.writer(self._dest_fp)
        for line in self._get_lines():
            if self._idx == -1:
                self._dest_csv.writerow(['d3mIndex'] + line)
            else:
                self._dest_csv.writerow([self._idx] + line)
            self._idx += 1

    def close(self):
        if self._generate is not False:
            if self._decoder is not None:
                # Flush decoder
                self._buffer.write(self._decoder.decode(b'', True))
            self._flush()
            if self._generate is True:
                # Write last line
                self._buffer.seek(0, 0)
                line = next(iter(csv.reader(self._buffer)))
                if line:
                    self._dest_csv.writerow([self._idx] + line)
        self._dest_fp.close()
        self._dest_csv = None
        self._dest_fp = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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

    def open_file(self, mode='wb', name=None):
        if name is not None:
            raise ValueError("D3mWriter can only write single-table datasets "
                             "for now")
        if self.need_d3mindex:
            fp = open(
                os.path.join(self.destination, 'tables', 'learningData.csv'),
                'w',
                encoding='utf-8',
                newline='',
            )
            fp = _D3mAddIndex(fp, 'b' in mode)
            return fp
        else:
            return open(
                os.path.join(self.destination, 'tables', 'learningData.csv'),
                mode,
            )

    def finish(self):
        return None
