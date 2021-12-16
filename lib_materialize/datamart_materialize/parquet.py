import fastparquet

from datamart_materialize.utils import SimpleConverter


def parquet_to_csv(source_filename, dest_fileobj):
    src = fastparquet.ParquetFile(source_filename)
    for i, chunk in enumerate(src.iter_row_groups()):
        chunk.to_csv(
            dest_fileobj,
            header=(i == 0),
            float_format='%g',
            date_format='%Y-%m-%dT%H:%M:%S',
            index=False,
            line_terminator='\r\n',
        )


class ParquetConverter(SimpleConverter):
    """Adapter pivoting a table.
    """
    def transform(self, source_filename, dest_fileobj):
        parquet_to_csv(
            source_filename,
            dest_fileobj,
        )
