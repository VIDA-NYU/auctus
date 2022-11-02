import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'aio-pika',
    'elasticsearch~=7.0',
    'prometheus_client',
    'PyYaml',
    'jinja2>=3,<4',
    'tornado>=5.0',
    'datamart-core',
]
setup(name='datamart-coordinator-service',
      version='0.0',
      packages=['coordinator'],
      package_data={'coordinator': [
          'static/css/*.css', 'static/css/*.css.map',
          'static/js/*.js', 'static/js/*.js.map',
          'templates/*.html',
          'elasticsearch.yml',
      ]},
      entry_points={
          'console_scripts': [
              'coordinator = coordinator.web:main']},
      install_requires=req,
      description="Coordinator service for Auctus",
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
      long_description="Coordinator service for Auctus",
      license='Apache-2.0',
      keywords=['auctus', 'datamart'],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: Unix',
          'Programming Language :: JavaScript',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
