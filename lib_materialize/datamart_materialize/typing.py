import typing
from typing_extensions import Protocol


class WriterBase(Protocol):
    @typing.overload
    def open_file(
        self, mode: typing.Literal['wb'] = 'wb',
        name: typing.Optional[str] = None,
    ) -> typing.BinaryIO:
        raise NotImplementedError

    @typing.overload
    def open_file(
        self, mode: typing.Literal['w'],
        name: typing.Optional[str] = None,
        *,
        newline: typing.Optional[str] = None,
        encoding: typing.Optional[str] = None,
    ) -> typing.TextIO:
        raise NotImplementedError


class Writer(WriterBase, Protocol):
    def __init__(
        self,
        dataset_id: str, destination: str,
        metadata: typing.Dict[str, typing.Any],
        format_option: typing.Optional[typing.Dict[str, str]] = None,
    ):
        raise NotImplementedError
