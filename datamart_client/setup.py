import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'requests',
]
setup(name='datamart_client',
      version='0.0',
      py_modules=['datamart_client'],
      install_requires=req,
      description="Client library for DataMart",
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
      long_description="Client library for DataMart",
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
