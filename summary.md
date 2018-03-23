# KnoxPy Summary

This presentation will follow the story of a developer wanting to
create the a python package for analyzing malicious attempted ssh
logins to their home machine using an
[SQLite3](https://sqlite.org/index.html) database. In the end we will
have created a continuously delivered [PyPi](https://pypi.org/)
package that has documentation on
[readthedocs](https://readthedocs.org/) generated with
[sphinx](http://www.sphinx-doc.org/en/stable/) and tested via
[pytest](https://docs.pytest.org/en/latest/).

The talk will focus on showing the power of `sqlite3`
[module](https://docs.python.org/3/library/sqlite3.html) in the python
standard library. I will not assume that the user has used `SQL`
before and show examples of simple `SELECT`, `INSERT`, `CREATE TABLE`,
transactions, bulk inserts for performance, and how to determine if
SQLite3 is the correct database to use.

For advanced users this talk will still have equal benefit. I will
show how to use custom [SQLite
datatypes](https://www.sqlite.org/datatype3.html) for serialization
and deserialization of datetimes and JSON. Additionally, using a
unique feature of SQLite we will create python functions for queries
not possible with complex SQL statements.

This talk will be based on two of my blog posts

-   [sqlite with python](https://chrisostrouchov.com/post/python_sqlite/)
-   [continuous delivery, tested, and documented pypi packages](https://chrisostrouchov.com/post/cd_tested_doc_pypi_package/)
    (currently drafted will be available mid April)

Packages Used:

-   sqlite3 (detailed)
-   pytest (brief)
-   sphinx (brief)
-   setuptools (brief)

All source code and documents will be available in the [Gitlab
repo](https://gitlab.com/costrouc/knoxpy-sqlite-pypi-readthedocs)
