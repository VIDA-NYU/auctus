import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'aiohttp',
    'beautifulsoup4',
    'html5lib',
    'requests',
    'datamart_core',
    'jinja2',
    'tornado>=5.0',
]
setup(name='web_discovery',
      version='0.0',
      packages=['web_discovery'],
      package_data={'web_discovery': [
          'static/css/*.css', 'static/css/*.css.map',
          'static/js/*.js', 'static/js/*.js.map',
          'templates/*.html',
      ]},
      entry_points={
          'console_scripts': [
              'web_discovery = web_discovery:main']},
      install_requires=req,
      description="Web discovery service for DataMart",
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
      long_description="Web discovery service for DataMart",
      license='BSD-3-Clause',
      keywords=['datamart'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Science/Research',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Information Analysis'])
