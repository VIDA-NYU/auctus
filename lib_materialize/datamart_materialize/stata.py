import pandas

from datamart_materialize.utils import SimpleConverter


def stata_to_csv(source_filename, dest_fileobj):
    for i, chunk in enumerate(
        pandas.read_stata(source_filename, iterator=True, chunksize=1)
    ):
        chunk.to_csv(
            dest_fileobj,
            header=(i == 0),
            float_format='%g',
            date_format='%Y-%m-%dT%H:%M:%S',
            index=False,
            line_terminator='\r\n',
        )


class StataConverter(SimpleConverter):
    """Adapter converting a Stata file to CSV.
    """
    transform = staticmethod(stata_to_csv)
