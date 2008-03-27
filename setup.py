#!/usr/bin/env python

from distutils.core import setup

setup(name='TSDB',
      version='20071214',
      description='TSDB (Time Series Database)',
      author='Jon M. Dugan',
      author_email='jdugan@es.net',
      url='http://www.es.net',
      py_modules=['tsdb'],
      install_requires=['fpconst==0.7.2'],
     )

