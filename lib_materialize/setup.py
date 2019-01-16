import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'requests',
    'sodapy',
]
setup(name='datamart_materialize',
      version='0.1',
      packages=['datamart_materialize'],
      entry_points={
          'datamart_materialize': [
              'datamart.noaa = datamart_materialize.noaa:NoaaMaterializer',
          ],
          'datamart_materialize.writer': [
              'csv = datamart_materialize:CsvWriter',
              'd3m = datamart_materialize.d3m:D3mWriter',
          ],
      },
      install_requires=req,
      description="Materialization library for DataMart",
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
      long_description="Materialization library for DataMart",
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
