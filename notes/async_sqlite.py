#! /usr/bin/env python3

# Copyright 2020 Darren Weber
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Example code for async SQLite

.. seealso::
    - https://www.encode.io/databases/
    - https://www.sqlitetutorial.net/
"""

from databases import Database


async def sqlite_setup_db(database: Database):
    """
    Setup example SQLite database
    """

    if not database.is_connected:
        await database.connect()

    query = "DROP TABLE IF EXISTS HighScores"
    await database.execute(query=query)

    query = "CREATE TABLE HighScores (id INTEGER PRIMARY KEY, name VARCHAR(100), score INTEGER)"
    await database.execute(query=query)

    query = "INSERT INTO HighScores(name, score) VALUES (:name, :score)"
    values = [
        {"name": "Daisy", "score": 92},
        {"name": "Neil", "score": 87},
        {"name": "Carol", "score": 43},
    ]
    await database.execute_many(query=query, values=values)


async def sqlite_query_db(database: Database):
    """
    Run async query on SQLite

    :return: database records
    """
    if not database.is_connected:
        await database.connect()

    query = "SELECT * FROM HighScores"
    records = await database.fetch_all(query=query)
    return records


async def run_sqlite():
    """
    Run example async SQLite
    """

    database = Database("sqlite:///example.db")
    await sqlite_setup_db(database)
    records = await sqlite_query_db(database)
    print("High Scores:", records)


if __name__ == "__main__":

    from notes.async_main import main

    main(run_sqlite())
