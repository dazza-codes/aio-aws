
# Python Notes

This is a random collection of notes about working with python.
The notes are published as gitlab pages at:
- http://dazza-codes.gitlab.io/python-notes

# Getting Started

To use the source code in these notes it can be cloned directly. To
contribute to the project, first fork it and clone the forked repository.

The following setup assumes that
[miniconda3](https://docs.conda.io/en/latest/miniconda.html) and
[poetry](https://python-poetry.org/docs/) are installed already (and `make`
4.x).

- https://docs.conda.io/en/latest/miniconda.html
    - recommended for creating virtual environments with required versions of python
- https://python-poetry.org/docs/
    - recommended for managing a python project with pip dependencies for
      both the project itself and development dependencies

```shell
git clone https://gitlab.com/dazza-codes/python-notes
cd python-notes
conda create -n python-notes python=3.6
conda activate python-notes
make init  # calls poetry install
```
