import os
from setuptools import setup


os.chdir(os.path.abspath(os.path.dirname(__file__)))


req = [
    'prometheus_client',
]
setup(name='datamart-fslock',
      version='2.1',
      packages=['datamart_fslock'],
      install_requires=req,
      description="Filesystem locking library for Auctus",
      author="Remi Rampin",
      author_email='remi.rampin@nyu.edu',
      maintainer="Remi Rampin",
      maintainer_email='remi.rampin@nyu.edu',
      url='https://gitlab.com/remram44/python-fslock',
      project_urls={
          'Homepage': 'https://gitlab.com/remram44/python-fslock',
          'Source': 'https://gitlab.com/remram44/python-fslock',
          'Tracker': 'https://gitlab.com/remram44/python-fslock/issues',
      },
      long_description="Filesystem locking library for Auctus",
      license='MIT',
      keywords=['lock', 'flock', 'file lock', 'locking', 'filesystem'],
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 3 :: Only'])
