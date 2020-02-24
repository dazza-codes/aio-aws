
About
=====

See https://gitlab.com/dazza-codes/python-notes

Code for some examples can be run by cloning
that repo and following the instructions to get started.

The following setup assumes that
`miniconda3 <https://docs.conda.io/en/latest/miniconda.html>`_ and
`poetry <https://python-poetry.org/docs/>`_ are installed already (and `make`
4.x).

- https://docs.conda.io/en/latest/miniconda.html
    - recommended for creating virtual environments with
      required versions of python
- https://python-poetry.org/docs/
    - recommended for managing a python project with pip dependencies for
      both the project itself and development dependencies

.. code-block:: shell

    git clone https://gitlab.com/dazza-codes/python-notes
    cd python-notes
    conda create -n python-notes python=3.6
    conda activate python-notes
    make init  # calls poetry install


Acknowledgements
----------------

Some code in these notes could be inspired by various sources
and each set of notes will contain references to resources
consulted.  Also the project LICENCE.md file should contain
relevant details required for OSS citations (please open an
issue on the gitlab project for these notes if there are any
omissions that should be fixed).

PMOTW:

    - The PMOTW project contains many helpful
      examples at https://pymotw.com/3/index.html.

    .. note::
        To play with code from PYMOTW-3, it can be cloned using:
        ``git clone https://bitbucket.org/dhellmann/pymotw-3.git``
