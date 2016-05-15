from setuptools import setup, find_packages
from codecs import open
from os import path

VERSION = '1.2.0'

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='git-pandas',
    version=VERSION,
    description='A utility for interacting with data from git repositories as Pandas dataframes',
    long_description=long_description,
    url='https://github.com/wdm0006/git-pandas',
    download_url='https://github.com/wdm0006/git-pandas/tarball/' + VERSION,
    license='BSD',
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'Programming Language :: Python :: 3',
    ],
    keywords='git pandas data analysis',
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    author='Will McGinnis',
    install_requires=[
        'gitpython>=1.0.0',
        'numpy>=1.9.0',
        'pandas>=0.16.0'
    ],
    author_email='will@pedalwrencher.com'
)