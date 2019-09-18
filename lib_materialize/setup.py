import io
import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'requests',
    'sodapy',
    'xlrd',
]
with io.open('README.rst', encoding='utf-8') as fp:
    description = fp.read()
setup(name='datamart_materialize',
      version='0.5.1',
      packages=['datamart_materialize'],
      entry_points={
          'datamart_materialize': [
              'datamart.noaa = datamart_materialize.noaa:NoaaMaterializer',
          ],
          'datamart_materialize.writer': [
              'csv = datamart_materialize:CsvWriter',
              'd3m = datamart_materialize.d3m:D3mWriter',
          ],
          'datamart_materialize.converter': [
              'xls = datamart_materialize.excel:ExcelConverter',
          ],
      },
      install_requires=req,
      description="Materialization library for Datamart",
      long_description=description,
      author="Remi Rampin",
      author_email='remi.rampin@nyu.edu',
      maintainer="Remi Rampin",
      maintainer_email='remi.rampin@nyu.edu',
      url='https://gitlab.com/ViDA-NYU/datamart/datamart',
      project_urls={
          'Homepage': 'https://gitlab.com/ViDA-NYU/datamart/datamart',
          'Source': 'https://gitlab.com/ViDA-NYU/datamart/datamart',
          'Tracker': 'https://gitlab.com/ViDA-NYU/datamart/datamart/issues',
      },
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
