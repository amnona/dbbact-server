#!/usr/bin/env python

# ----------------------------------------------------------------------------
# dbbact-server
# The rest-API server for dbBact microbiome knowledge-base
# for more details, see dbbact.org
# ----------------------------------------------------------------------------

import re
import ast
from setuptools import setup

from os.path import join, dirname

# get the requirements. pulled from flask-cors setup.py
# https://github.com/corydolphin/flask-cors/blob/master/setup.py
with open(join(dirname(__file__), 'requirements.txt'), 'r') as f:
    install_requires = f.read().split("\n")


# version parsing from __init__ pulled from Flask's setup.py
# https://github.com/mitsuhiko/flask/blob/master/setup.py
_version_re = re.compile(r'__version__\s+=\s+(.*)')
with open('dbbact_server/__init__.py', 'rb') as f:
    hit = _version_re.search(f.read().decode('utf-8')).group(1)
    version = str(ast.literal_eval(hit))


classifiers = [
    'Development Status :: 2 - Pre-Alpha',
    'License :: OSI Approved :: MIT License',
    'Environment :: Console',
    'Topic :: Software Development :: Libraries :: Application Frameworks',
    'Topic :: Scientific/Engineering',
    'Topic :: Scientific/Engineering :: Bio-Informatics',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Operating System :: Unix',
    'Operating System :: POSIX',
    'Operating System :: MacOS :: MacOS X',
    'Operating System :: Microsoft :: Windows']


description = 'dbbact-server: the rest-API server for dbBact microbiome knowledge-base'

with open('README.md') as f:
    long_description = f.read()

keywords = 'microbiome analysis bioinformatics server'

setup(name='dbbact-server',
      version=version,
      license='BSD',
      description=description,
      long_description=long_description,
      keywords=keywords,
      classifiers=classifiers,
      author="dbBact development team",
      author_email='amnonim@gmail.com',
      maintainer="dbBact development team",
      url='http://dbbact.org',
      packages=['dbbact_server'],
      # package_data={'dbbact-server': ['log.cfg', 'dbbact.config']},
      install_requires=install_requires,
      # extras_require={'test': ["nose", "pep8", "flake8"],
      #                 'coverage': ["coveralls"],
      #                 'doc': ["Sphinx >= 1.4", "sphinx-autodoc-typehints", "nbsphinx"]}
      )
