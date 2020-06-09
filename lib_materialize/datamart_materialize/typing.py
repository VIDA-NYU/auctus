import typing
from typing import Any
from typing_extensions import Protocol


# Feel like duplicates but typing.TextIO and typing.BinaryIO are not Protocol

T = typing.TypeVar('T', str, bytes)


class WriteIO(Protocol[T]):
    def write(self, buf: T) -> int: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> 'WriteIO[T]': ...
    def __exit__(self, exc: Any, value: Any, tb: Any) -> None: ...


class ReadIO(Protocol[T]):
    def read(self, size: typing.Optional[int] = None) -> T: ...
    def close(self) -> None: ...
    def __enter__(self) -> 'WriteIO[T]': ...
    def __exit__(self, exc: Any, value: Any, tb: Any) -> None: ...


class WriterBase(Protocol):
    @typing.overload
    def open_file(
        self, mode: typing.Literal['wb'],
        name: typing.Optional[str] = None,
    ) -> WriteIO[bytes]:
        raise NotImplementedError

    @typing.overload
    def open_file(
        self, mode: typing.Literal['w'],
        name: typing.Optional[str] = None,
    ) -> WriteIO[str]:
        raise NotImplementedError

    def open_file(
        self, mode: str = 'wb',
        name: typing.Optional[str] = None,
    ) -> typing.Union[WriteIO[str], WriteIO[bytes]]:
        raise NotImplementedError


class Writer(WriterBase, Protocol):
    def __init__(
        self,
        dataset_id: str, destination: str,
        metadata: typing.Dict[str, typing.Any],
        format_option: typing.Optional[typing.Dict[str, str]] = None,
    ):
        raise NotImplementedError


# Not supported by mypy yet: https://github.com/python/mypy/issues/731
# JSON = typing.Dict[
#     str,
#     typing.Union[str, int, float, 'JSON'],
# ]
JSON = typing.Dict[str, typing.Any]
