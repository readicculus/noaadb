from distutils import util

from setuptools import setup, find_packages

schema = util.convert_path('noaadb/schema')

setup(
    name='noaadb',
    version='0.0.2',
    author='Yuval Boss',
    author_email='yuval@cs.washington.edu',
    packages=['noaadb','noaadb.api', 'noaadb.schema', 'noaadb.schema.utils', 'noaadb.schema.models'],
    # packages=['noaadb'],
    # package_dir={
    #     'noaadb': 'noaadb/src/noaadb',
    # },
    # packages=['noaadb', 'noaadb.schema'],
    # packages=find_packages(),

    scripts=[],
    url='https://github.com/readicculus/noaadb',
    license='LICENSE.txt',
    description='Package for querying animal bounding box labels and other metadata',
    long_description="Package for arctic survey and label data",
    install_requires=[
       "sqlalchemy >= 1.3.13",
       "psycopg2 >= 2.8.4",
       "python-dotenv >= 0.14.0",
       "pymongo >= 3.10.1",
    ],
    python_requires='>=3.6.8',
)