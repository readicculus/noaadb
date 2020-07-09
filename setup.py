from setuptools import setup

setup(
   name='noaadb',
   version='0.0.1',
   author='Yuval Boss',
   author_email='yuval@cs.washington.edu',
   packages=['noaadb','noaadb.api', 'noaadb.schema', 'noaadb.utils'],
   scripts=[],
   url='git@github.com:readicculus/noaadb.git',
   license='LICENSE.txt',
   description='Package for querying animal bounding box labels and other metadata',
   long_description=open('README.md').read(),
   install_requires=[
       "sqlalchemy >= 1.3.13",
       "psycopg2",
   ],
)