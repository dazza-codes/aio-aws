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
    - https://aioredis.readthedocs.io/en/latest/
"""

# import asyncio
import aioredis  # type: ignore


async def run_aio_redis():
    redis = await aioredis.create_redis_pool('redis://localhost')

    await redis.set('key', 'string-value')
    bin_value = await redis.get('key')
    assert bin_value == b'string-value'

    str_value = await redis.get('key', encoding='utf-8')
    assert str_value == 'string-value'
    print(str_value)

    redis.close()
    await redis.wait_closed()


if __name__ == "__main__":

    # asyncio.run(run_aio_redis())  # py > 3.6 ?

    from notes.async_main import main

    main(run_aio_redis())
