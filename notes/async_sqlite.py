#! /usr/bin/env python3

"""
Dummy code for async SQLite

.. seealso::
    - https://www.encode.io/databases/
"""

from databases import Database

from notes.async_main import main


async def run():
    """
    Run dummy async query on SQLite

    :return: database records
    """

    # Create a database instance, and connect to it.
    database = Database("sqlite:///example.db")
    await database.connect()

    # Create a table.
    query = (
        """CREATE TABLE HighScores (id INTEGER PRIMARY KEY, name VARCHAR(100), score INTEGER)"""
    )
    await database.execute(query=query)

    # Insert some data.
    query = "INSERT INTO HighScores(name, score) VALUES (:name, :score)"
    values = [
        {"name": "Daisy", "score": 92},
        {"name": "Neil", "score": 87},
        {"name": "Carol", "score": 43},
    ]
    await database.execute_many(query=query, values=values)

    # Run a database query.
    query = "SELECT * FROM HighScores"
    rows = await database.fetch_all(query=query)
    print("High Scores:", rows)


if __name__ == "__main__":

    main(run())
