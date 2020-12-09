import io
import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'requests',
    'xlrd',
    'pyreadstat>=1.0,<2.0',
]
with io.open('README.rst', encoding='utf-8') as fp:
    description = fp.read()
setup(name='auctus-materialize',
      version='0.8.1',
      packages=['auctus_materialize'],
      entry_points={
          'auctus_materialize': [
              'datamart.noaa = auctus_materialize.noaa:noaa_materializer',
          ],
          'auctus_materialize.writer': [
              'csv = auctus_materialize:CsvWriter',
              'd3m = auctus_materialize.d3m:D3mWriter',
              'pandas = auctus_materialize:PandasWriter',
          ],
          'auctus_materialize.converter': [
              'skip_rows = auctus_materialize.common:SkipRowsConverter',
              'xls = auctus_materialize.excel:ExcelConverter',
              'pivot = auctus_materialize.pivot:PivotConverter',
              'spss = auctus_materialize.spss:SpssConverter',
              'stata = auctus_materialize.stata:StataConverter',
              'tsv = auctus_materialize.tsv:TsvConverter',
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
