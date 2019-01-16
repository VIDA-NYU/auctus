import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'requests',
    'pandas'
]
setup(name='datamart',
      version='0.1',
      py_modules=['datamart'],
      install_requires=req,
      description="Client library for DataMart",
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
      long_description="Client library for DataMart",
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Topic :: Scientific/Engineering :: Information Analysis'])
