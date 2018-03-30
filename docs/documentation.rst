Documentation
=============

Documenting a python project is a daunting task. Even though I myself
have had experience with documentation it always takes me time to get
the setup just right. In python the standard way to create
documentation is with `Sphinx
<http://www.sphinx-doc.org/en/master/>`_. Sphinx is not
straightforward to use and relies heavily on `restructured text
<https://en.wikipedia.org/wiki/ReStructuredText>`_. Restructured text
is a somewhat more verbose markup language than Markdown but is not
too hard to `learn the syntax
<http://docutils.sourceforge.net/docs/user/rst/quickref.html>`_. There
is a lot to learn to use RST properly with python sadly. Most of the
installation instructions follow the awesome `An idiot’s guide to
Python documentation with Sphinx and ReadTheDocs
<https://samnicholls.net/2016/06/15/how-to-sphinx-readthedocs/>`_. My
changes are that I want to show how to include docstring in the google
format and how to additionally deploy documentation on a static site
without readthedocs.

First you will install sphinx and create a docs folder in the root of
your project. If you want to use the readthedocs theme for the
documentation you will need to install the `sphinx_rtd_theme
<https://github.com/rtfd/sphinx_rtd_theme>`_.

1. ``pip install sphinx sphinx_rtd_theme``
2. ``mkdir docs``

Next you will want to setup a basic sphinx project. You will do this
by running the command `sphinx-quickstart` within the ``docs`` folder.
Most of the default options are good. You will need to set a ``project
name``, ``version``, and answer yes to ``autodoc`` since we want our
project source code to be documented. At this point you will have very
basic sphinx documentation. Next we need to add our package source
documentation. This can be done via the ``sphinx`` tool
``sphinx-autodoc``. Add the following to your ``Makefile`` in the
``docs``. Now run ``make apidocs`` to add the outline of your source
code.

-------------
docs/Makefile
-------------

.. code-block:: shell

   apidocs:
	sphinx-apidoc -o source/ ../<package>

If you wanted the readthedocs theme instead of the default you will need to modify ``docs/conf.py``.

.. code-block:: python

   ...
   html_theme = 'sphinx_rtd_theme'
   ...

The default sphinx apidoc is tedious and I recommend using `sphinx
napolean docstrings
<http://www.sphinx-doc.org/en/stable/ext/napoleon.html>`_. Here is an
example of simple function being documented in the google style. See
the `google docstring format
<https://google.github.io/styleguide/pyguide.html?showone=Comments#Comments>`_
for further details.

.. code-block:: python

   def fizzbuzz(n):
       """A super advanced fizzbuzz function

       Write a program that prints the numbers from 1 to 100. But for
       multiples of three print “Fizz” instead of the number and for the
       multiples of five print “Buzz”. For numbers which are multiples of
       both three and five print “FizzBuzz” Prints out fizz and buzz

       Args:
           n (int): number for fizzbuzz to count to

       Returns:
          None: prints to stdout fizzbuzz
       """
       def _fizzbuzz(i):
           if i % 3 == 0 and i % 5 == 0:
               return 'FizzBuzz'
           elif i % 3 == 0:
               return 'Fizz'
           elif i % 5 == 0:
               return 'Buzz'
           else:
               return str(i)
       print("\n".join(_fizzbuzz(i+1) for i in range(n)))


At this point you are ready to go! You can run ``make html`` within
the docs folder and it will build the website in
``docs/_build/html``. Okay so great we have the static files for the
website but how do I deploy them?! There are two answers and you can
choose both: self hosting and `readthedocs.org
<https://readthedocs.org>`_.

---------------
readthedocs.org
---------------

First you will signup an account with readthedocs.org. It is not
necessary to link an account as readthedocs will work with any
publicly available version controlled repo. ``Import a project ->
Import Manually`` and give the project a unique name and specify the
repository url. The name that you provide determines the url
``<name>.readthedocs.org``. For full documentation see the
``https://docs.readthedocs.io/en/latest/getting_started.html``.

Readthedocs will detect and change in the repository and rebuilt the
documentation. However often times the default configuration does not
work with cutting edge projects and also by default does not install
the project when building the documentation. To specify the
readthedocs configuration in your project you should use
`.readthedocs.yml
<https://docs.readthedocs.io/en/latest/yaml-config.html?highlight=.readthedocs.yml>`_. A basic configuration is specified below. Readthedocs uses docker containers and has many more configuration options. With this you should be all setup! Read the documentation for additional options.

----------------
.readthedocs.yml
----------------

.. code-block:: yaml

   build:
     image: latest

   python:
     version: 3.6
     setup_py_install: true


-------------------------
static documentation site
-------------------------

Sometime it is nicer to just deploy the static website yourself. Read
the docs is an awesome resource but it does have limitations. For
instance one issue I have had is that it does not generate docstrings
from cextensions such as `cython <http://cython.org/>`_ code. In these
cases we can use Gitlab CD/CI for deploying our own static site.

Since we already have a pipeline for our project lets include the
static website building. Add the following to ``.gitlab-ci.yml``

--------------
.gitlab-ci.yml
--------------

.. code-block:: yaml

   stages:
     - test
     - deploy
     - docs

   pages:
     image: python:3.6
     stage: docs
     script:
       - pip install sphinx sphinx_rtd_theme
       - mkdir public
       - cd docs
       - make apidocs
       - make html
       - cp -r _build/html ../public
     artifacts:
       paths:
         - public
     only:
       - master

We are using `gitlab pages
<https://docs.gitlab.com/ee/user/project/pages/index.html>`_ to deploy
our website. It should be available at
``<username>.gitlab.io/<repo>``. If you would like to add a custom
domain follow either my blog at `gitlab static site deployment
<https://chrisostrouchov.com/posts/hugo_static_site_deployment/>`_ or
look at the `gitlab cloudflare documentation
<https://about.gitlab.com/2017/02/07/setting-up-gitlab-pages-with-cloudflare-certificates/>`_.

Now you have your documentation completed!
