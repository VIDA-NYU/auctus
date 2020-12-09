import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'aio-pika',
    'StringDist',
    'elasticsearch~=7.0',
    'redis~=3.4',
    'lazo-index-service==0.6.1',
    'prometheus_client',
    'tornado>=5.0',
    'auctus-data-augmentation',
    'auctus-server-core',
    'auctus-materialize',
    'auctus-data-profiler',
]
setup(name='auctus-api-service',
      version='0.0',
      packages=['apiserver'],
      entry_points={
          'console_scripts': [
              'apiserver = apiserver.main:main']},
      install_requires=req,
      description="API service of Auctus",
      author="Remi Rampin",
      author_email='remi.rampin@nyu.edu',
      maintainer="Remi Rampin",
      maintainer_email='remi.rampin@nyu.edu',
      url='https://gitlab.com/ViDA-NYU/auctus/auctus',
      project_urls={
          'Homepage': 'https://gitlab.com/ViDA-NYU/auctus/auctus',
          'Source': 'https://gitlab.com/ViDA-NYU/auctus/auctus',
          'Tracker': 'https://gitlab.com/ViDA-NYU/auctus/auctus/-/issues',
      },
      long_description="API service of Auctus",
      license='Apache-2.0',
      keywords=['auctus', 'datamart'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: Unix',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
