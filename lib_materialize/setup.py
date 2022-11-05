import io
import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'requests',
    'xlrd',
    'openpyxl',
    'lxml',  # optional dependency for openpyxl, it's slow without it
    'fastparquet>=0.7,<0.8',
    'pyreadstat>=1.0,<2.0',
]
with io.open('README.rst', encoding='utf-8') as fp:
    description = fp.read()
setup(name='datamart-materialize',
      version='0.11',
      packages=['datamart_materialize'],
      entry_points={
          'datamart_materialize': [
              'datamart.noaa = datamart_materialize.noaa:noaa_materializer',
          ],
          'datamart_materialize.writer': [
              'csv = datamart_materialize:CsvWriter',
              'd3m = datamart_materialize.d3m:D3mWriter',
              'pandas = datamart_materialize:PandasWriter',
          ],
          'datamart_materialize.converter': [
              'skip_rows = datamart_materialize.common:SkipRowsConverter',
              'xls = datamart_materialize.excel97:Excel97Converter',
              'xlsx = datamart_materialize.excel:ExcelConverter',
              'parquet = datamart_materialize.parquet:ParquetConverter',
              'pivot = datamart_materialize.pivot:PivotConverter',
              'spss = datamart_materialize.spss:SpssConverter',
              'stata = datamart_materialize.stata:StataConverter',
              'tsv = datamart_materialize.tsv:TsvConverter',
          ],
      },
      install_requires=req,
      description="Materialization library for Auctus",
      long_description=description,
      author="Remi Rampin",
      author_email='remi.rampin@nyu.edu',
      maintainer="Remi Rampin",
      maintainer_email='remi.rampin@nyu.edu',
      url='https://gitlab.com/ViDA-NYU/auctus/auctus',
      project_urls={
          'Homepage': 'https://gitlab.com/ViDA-NYU/auctus/auctus',
          'Source': 'https://gitlab.com/ViDA-NYU/auctus/auctus',
          'Tracker': 'https://gitlab.com/ViDA-NYU/auctus/auctus/issues',
      },
      license='Apache-2.0',
      keywords=['auctus', 'datamart'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
