#!/usr/bin/env python

from distutils.core import setup

setup(name='TSDB',
      version='0.9b1',
      description='TSDB (Time Series Database)',
      author='Jon M. Dugan',
      author_email='jdugan@es.net',
      url='http://code.google.com/p/tsdb/',
      packages=['tsdb'],
      install_requires=['fpconst==0.7.2'],
      dependency_links=['http://pyfilesystem.googlecode.com/svn/trunk/#egg=fs'],
      entry_points = {
          'console_scripts': [
              'tsdb = tsdb.cli:main',
            ]
        }
     )
