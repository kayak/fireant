# coding: utf8

from setuptools import setup

from fireant import __version__

setup(
    name='fireant',
    version=__version__,

    author='KAYAK, GmbH',
    author_email='bandit@kayak.com',

    # License
    license='Apache License Version 2.0',

    packages=['fireant', 'fireant.database', 'fireant.dashboards', 'fireant.slicer', 'fireant.slicer.transformers'],

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

    install_requires=[
        'six',
        'pandas==0.18.1',
        'pypika==0.1.12'
    ],
    tests_require=[
        'mock'
    ],
    extras_require={
        'vertica': ['vertica-python>=0.6'],
        'matplotlib': ['matplotlib'],
    },

    test_suite='fireant.tests',
)
