import csv
import os
import tempfile
import xlrd


def xls_to_csv(source_filename, dest_fileobj):
    workbook = xlrd.open_workbook(source_filename)
    sheets = workbook.sheets()
    if len(sheets) != 1:
        raise ValueError("Excel workbook has %d sheets" % len(sheets))
    sheet, = sheets

    writer = csv.writer(dest_fileobj)
    for row_num in range(sheet.nrows):
        writer.writerow(sheet.row_values(row_num))


class _ExcelProxy(object):
    def __init__(self, writer, name, temp_file, fp):
        self._writer = writer
        self._name = name
        self._temp_file = temp_file
        self._fp = fp

    def close(self):
        self._fp.close()

        # Read back the XLS file we wrote, and write CSV via the writer
        with self._writer.open_file('w', self._name, newline='') as dst:
            xls_to_csv(self._temp_file, dst)

    # Those methods forward to the actual file object

    def write(self, buffer):
        return self._fp.write(buffer)

    def flush(self):
        self._fp.flush()

    def __enter__(self):
        self._fp.__enter__()
        return self

    def __exit__(self, exc, value, tb):
        self._fp.__exit__(exc, value, tb)
        self.close()


class ExcelConverter(object):
    """Adapter converting Excel files to CSV.
    """
    def __init__(self, writer):
        self.writer = writer
        self.dir = tempfile.TemporaryDirectory(prefix='datamart_excel_')

    def open_file(self, mode='wb', name=None, **kwargs):
        temp_file = os.path.join(self.dir.name, 'file.xls')

        # Return a proxy that will write to the destination when closed
        fp = open(temp_file, mode, **kwargs)
        return _ExcelProxy(self.writer, name, temp_file, fp)

    def finish(self):
        self.dir.cleanup()
        self.dir = None
