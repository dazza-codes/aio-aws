
About
=====

See https://github.com/dazza-codes/aio-aws

Code for some examples can be run by cloning
the repo and following the instructions to get started.

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

    git clone https://github.com/dazza-codes/aio-aws
    cd aio-aws
    conda create -n aio-aws python=3.7
    conda activate aio-aws
    make init  # calls poetry install
