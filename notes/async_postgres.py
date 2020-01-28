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
Example code for asyncpg

.. seealso::
    - https://github.com/magicstack/asyncpg#basic-usage
"""

import asyncpg  # type: ignore


async def run_asyncpg():
    """
    Run example queries on PostgreSQL

    :return: database records
    """
    conn = await asyncpg.connect(
        user="user", password="password", database="database", host="127.0.0.1"
    )
    records = await conn.fetch("SELECT * FROM mytable")
    await conn.close()
    return records


if __name__ == "__main__":

    from notes.async_main import main

    main(run_asyncpg())
