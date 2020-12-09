import pyreadstat

from datamart_materialize.utils import SimpleConverter


def spss_to_csv(source_filename, dest_fileobj):
    df, meta = pyreadstat.read_sav(source_filename)
    df.to_csv(
        dest_fileobj,
        float_format='%g',
        index=False,
        line_terminator='\r\n',
    )


class SpssConverter(SimpleConverter):
    """Adapter converting an SPSS file to CSV.
    """
    transform = staticmethod(spss_to_csv)
