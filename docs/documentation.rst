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

At this point you are ready to go! You can run ``make html`` within
the docs folder and it will build the website in
``docs/_build/html``. If you wanted the readthedocs theme instead of the default you will need to modify ``docs/conf.py``.

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



Okay so great we have the static files for the website but how do I
deploy them?! There are two answers: self hosting and `readthedocs.org
<https://readthedocs.org>`_.

===============
readthedocs.org
===============

First you will create a






`yaml configuration readthedocs <https://docs.readthedocs.io/en/latest/yaml-config.html?highlight=.readthedocs.yml>`


sphinx_rtd_theme use it. ``pip install sphinx_rtd_theme``.
