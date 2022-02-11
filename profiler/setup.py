import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'aio-pika',
    'elasticsearch~=7.0',
    'lazo-index-service==0.7.0',
    'opentelemetry-api',
    'opentelemetry-distro',
    'opentelemetry-instrumentation-elasticsearch',
    'opentelemetry-instrumentation-grpc',
    'prometheus_client',
    'xlrd',
    'defusedxml',
    'datamart-core',
    'datamart-materialize',
    'datamart-profiler',
]
setup(name='datamart-profiler-service',
      version='0.0',
      py_modules=['profiler'],
      entry_points={
          'console_scripts': [
              'profiler = profiler:main']},
      install_requires=req,
      description="Data profiling service of Auctus",
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
      long_description="Data profiling service of Auctus",
      license='Apache-2.0',
      keywords=['auctus', 'datamart'],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: Apache Software License',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
