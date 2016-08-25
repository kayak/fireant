# coding: utf8

from setuptools import setup

__major_version__ = 0
__minor_version__ = 0
__patch_version__ = 14


def readme():
    with open('README.rst') as f:
        return f.read()


setup(
    name='fireant',
    version='{major}.{minor}.{patch}'.format(major=__major_version__,
                                             minor=__minor_version__,
                                             patch=__patch_version__),

    author='KAYAK, GmbH',
    author_email='bandit@kayak.com',

    # License
    license='Apache License Version 2.0',

    packages=['fireant', 'fireant.database', 'fireant.dashboards', 'fireant.slicer', 'fireant.slicer.transformers'],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url='https://github.com/kayak/fireant',

    description='A data analysis tool for Python and Jupyter Notebooks',
    long_description=readme(),

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
        'pandas',
        'pypika==0.0.27'
    ],
    tests_require=[
        'mock', 'vertica-python>=0.6'
    ],
    extras_require={
        'vertica': ['vertica-python>=0.6'],
        'notebook': ['matplotlib'],
    },

    test_suite='fireant.tests',
)
