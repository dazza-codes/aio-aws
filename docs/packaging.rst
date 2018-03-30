Packaging
=========

In this section I will talk about how create a simple python package
that can be installed using ``python setup.py install``. These are the
basics sharing your package with other users. In order to get your
package to install with ``pip`` you will need to complete the steps in
this guide and :doc:`pypi`. The reason is that this guide only shows
how to let someone install your package if they have the package
directory on their machine.

This guide was taken from several resources:

 - `setup.py reference documentation <https://setuptools.readthedocs.io/en/latest/setuptools.html>`
 - `pypi sample project <https://github.com/pypa/sampleproject>`
 - `kennethreitz setup.py <https://github.com/kennethreitz/setup.py>`
 - `pypi suports markdown <https://dustingram.com/articles/2018/03/16/markdown-descriptions-on-pypi>`

Is anyone else troubled by the fact that so many links are necissary
for simple python package development?!

The most important file is the ``setup.py`` file. All required and
optional fields are given ``<required>`` and ``<optional>``
respectively.

.. code-block:: python

    from setuptools import setup, find_packages
    from codecs import open
    from os import path

    here = path.abspath(path.dirname(__file__))

    # Get the long description from the README file
    with open(path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()

    setup(
        name='<required>',
        version='<required>',
        description='<required>',
        long_description=long_description,
        long_description_content_type="text/markdown",
        url='<optional>',
        author='<optional>',
        author_email='<optional>',
        license='<optional>',
        classifiers=[
            # Trove classifiers
            # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: Implementation :: CPython'
        ],
        keywords='<optional>',
        packages=find_packages(exclude=['docs', 'tests']),
        # setuptools > 38.6.0 needed for markdown README.md
        setup_requires=['setuptools>=38.6.0'],
    )

While `setuptools docs
<https://setuptools.readthedocs.io/en/latest/setuptools.html>` detail
each option. I still needed some of the keyworks in more plain
english.

name
  the name of package on pypi and when listed in pip. This is not
  the name of the package that you import via python. The name of the
  import will always be the name of the package directory for example
  ``pypkgtemp``.

version
  make sure that the version numbers when pushing to pypi are unique. Also best to
  follow `semantic versioning <https://semver.org/>`.

description
  keep it short and describe your package

long_description
  make sure that you have created a README.md file in
  the project directory. Why use a README.md instead of README.rst?
  It's simple, Github, Bitbucket, Gitlab, etc. all will display a
  README.md as the homepage.

url
  link to git repo url

author
  give yourself credit!

author_email
  nobody should really use this address to contact you about the package

license
  need help choosing a license? use `choosealicense <https://choosealicense.com/>`

classifiers
  one day would be nice to know why they are important. list of available `tags <https://pypi.python.org/pypi?%3Aaction=list_classifiers>`.

keywords
  will help with searching for package on pypi

packages
  which packages to include in python packaging. using
  ``find_packages`` is very helpful.


setup_requires
  list of packages required for setup. Note that versioning uses `environment markers <https://www.python.org/dev/peps/pep-0508/#environment-markers>`.
