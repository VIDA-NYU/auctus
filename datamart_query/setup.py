import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'elasticsearch',
    'tornado>=5.0',
]
setup(name='datamart_query',
      version='0.0',
      packages=['datamart_query'],
      entry_points={
          'console_scripts': [
              'datamart_query = datamart_query.web:main']},
      install_requires=req,
      description="Query component of DataMart",
      author="Remi Rampin",
      author_email='remi.rampin@nyu.edu',
      maintainer="Remi Rampin",
      maintainer_email='remi.rampin@nyu.edu',
      url='https://gitlab.com/remram44/datamart',
      project_urls={
          'Homepage': 'https://gitlab.com/remram44/datamart',
          'Source': 'https://gitlab.com/remram44/datamart',
          'Tracker': 'https://gitlab.com/remram44/datamart/issues',
      },
      long_description="Query component of DataMart",
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
