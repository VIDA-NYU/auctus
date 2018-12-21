import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'aio-pika',
    'datamart_core',
    'Distance',
    'elasticsearch',
]
setup(name='datamart_core',
      version='0.0',
      packages=['datamart_core'],
      install_requires=req,
      description="Core library for DataMart services",
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
      long_description="Core library for DataMart services",
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
