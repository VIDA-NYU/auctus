import csv

from datamart_materialize.utils import SimpleConverter


class UnsupportedConversion(ValueError):
    """This conversion cannot work."""


def skip_rows(source_filename, dest_fileobj, nb_rows):
    with open(source_filename, 'r') as src_fp:
        src = iter(csv.reader(src_fp))
        dst = csv.writer(dest_fileobj)

        # Skip rows
        for i in range(nb_rows):
            try:
                next(src)
            except StopIteration:
                raise ValueError(
                    "Can't skip %d rows, table only has %d" % (nb_rows, i),
                )

        # Copy rest
        for row in src:
            dst.writerow(row)


class SkipRowsConverter(SimpleConverter):
    """Adapter skipping a given number of rows from a CSV file.
    """
    def __init__(self, writer, *, nb_rows):
        super(SkipRowsConverter, self).__init__(writer)
        self.nb_rows = nb_rows

    def transform(self, source_filename, dest_fileobj):
        skip_rows(source_filename, dest_fileobj, self.nb_rows)
