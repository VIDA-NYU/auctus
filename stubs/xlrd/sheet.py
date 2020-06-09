import typing


class Sheet(object):
    nrows: int

    def row_values(self, rowx: int) -> typing.List[str]:
        ...
