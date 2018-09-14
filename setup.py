import codecs
import os
import re

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='fireant',
    version=find_version("fireant", "__init__.py"),

    author='KAYAK, GmbH',
    author_email='bandit@kayak.com',

    # License
    license='Apache License Version 2.0',

    packages=['fireant',
              'fireant.database',
              'fireant.slicer',
              'fireant.slicer.queries',
              'fireant.slicer.widgets'],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url='https://github.com/kayak/fireant',

    description='Data analysis tool for Python and Jupyter Notebooks',
    long_description="Fireant is a tool which aims to simplify the process of accessing analytical data and building "
                     "dashboards and reports.  It eliminates the need for handwritten queries or code to process data. "
                     "The main feature, the slicer, is a schema that is used to configure metrics, the data being "
                     "visualized, and dimensions, the spatiality over which metrics can be extended and grouped. "
                     "Included in the Slicer is a Slicer Manager API which exposes methods to render tables and charts "
                     "in a variety of different formats.\n\n"

                     "Fireant is a great compliment to Jupter notebooks using matplotlib and pandas. It can be used in "
                     "web projects with Highcharts and Datatables.js.  It can even just provide a layer of abstraction "
                     "for accessing analytical data akin to ORM libraries in software projects.",

    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: PL/SQL',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
    ],
    keywords=('fireant python query builder querybuilder sql mysql postgres psql oracle vertica aggregated '
              'relational database rdbms business analytics bi data science analysis pandas'),

    install_requires=required,
    tests_require=[
        'mock'
    ],
    extras_require={
        'vertica': ['vertica-python==0.7.3'],
        'mysql': ['pymysql==0.8.0'],
        'redshift': ['psycopg2==2.7.3.2'],
        'postgresql': ['psycopg2==2.7.3.2'],
        'matplotlib': ['matplotlib'],
    },

    test_suite='fireant.tests',
)
