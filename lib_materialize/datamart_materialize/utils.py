import os
import tempfile
import typing

from .typing import WriterBase


T = typing.TypeVar('T', typing.TextIO, typing.BinaryIO)


class SimpleConverterProxy(typing.Generic[T]):
    def __init__(
        self,
        writer: WriterBase,
        transform: typing.Callable[[str, typing.TextIO], None],
        name: str,
        temp_file: str,
        fp: T,
    ):
        self._writer = writer
        self._transform = transform
        self._name = name
        self._temp_file = temp_file
        self._fp: T = fp

    def close(self) -> None:
        self._fp.close()
        self._convert()

    def _convert(self) -> None:
        # Read back the file we wrote, and transform it to the final file
        with self._writer.open_file('w', self._name, newline='') as dst:
            self._transform(self._temp_file, dst)

    # Those methods forward to the actual file object

    @typing.overload
    def write(self: 'SimpleConverterProxy[typing.BinaryIO]', buffer: bytes) -> int:
        ...

    @typing.overload
    def write(self: 'SimpleConverterProxy[typing.TextIO]', buffer: str) -> int:
        ...

    def write(self, buffer) -> int:
        return self._fp.write(buffer)

    def flush(self) -> None:
        self._fp.flush()

    def __enter__(self) -> 'SimpleConverterProxy[T]':
        self._fp.__enter__()
        return self

    def __exit__(self, exc: typing.Any, value: typing.Any, tb: typing.Any) -> None:
        self._fp.__exit__(exc, value, tb)
        if exc is None:
            self._convert()


class SimpleConverter(WriterBase):
    """Base class for converters simply transforming files through a function.
    """
    dir: typing.Optional[tempfile.TemporaryDirectory[str]]

    def __init__(self, writer: WriterBase):
        self.writer = writer
        self.dir = tempfile.TemporaryDirectory(prefix='datamart_excel_')

    def open_file(self, mode='wb', name=None, **kwargs):
        dir_name = typing.cast(tempfile.TemporaryDirectory[str], self.dir).name
        temp_file = os.path.join(dir_name, 'file.xls')

        # Return a proxy that will write to the destination when closed
        fp = open(temp_file, mode, **kwargs)
        return SimpleConverterProxy(
            self.writer, self.transform,
            name,
            temp_file, fp,
        )

    def finish(self) -> None:
        typing.cast(tempfile.TemporaryDirectory[str], self.dir).cleanup()
        self.dir = None

    @staticmethod
    def transform(source_filename: str, dest_fileobj: typing.TextIO) -> None:
        raise NotImplementedError
