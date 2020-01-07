#! /usr/bin/env python3

"""
Dummy code for asyncpg

.. seealso::
    - https://github.com/magicstack/asyncpg#basic-usage
"""

import asyncpg  # type: ignore


async def run():
    """
    Run dummy async query on PostgreSQL

    :return: database records
    """
    conn = await asyncpg.connect(
        user="user", password="password", database="database", host="127.0.0.1"
    )
    records = await conn.fetch("""SELECT * FROM mytable""")
    await conn.close()
    return records


if __name__ == "__main__":

    # cannot run without postgres setup
    # from notes.async_main import main
    # main(run())

    pass
