import typing

from xlrd.sheet import Sheet


class Book(object):
    def sheets(self) -> typing.List[Sheet]:
        ...
