# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='sshlog',
    version='0.0.4',
    description='A tool to analyze sshd logs',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://gitlab.com/costrouc/knoxpy-sqlite-pypi-readthedocs',
    author='Chris Ostrouchov',
    author_email='chris.ostrouchov+sshlog@gmail.com',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
    ],
    keywords='sqlite3 knoxpy ssh',
    packages=find_packages(exclude=['docs', 'tests', 'presentation']),
    setup_requires=['pytest-runner', 'setuptools>=38.6.0'],
    tests_require=['pytest']
)
