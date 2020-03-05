
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

# License

Copyright 2019-2020 Darren Weber

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
