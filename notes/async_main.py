"""
Basic async main loop
"""

import asyncio
from typing import Awaitable


def main(run: Awaitable):
    """Run main loop"""
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run)
    finally:
        loop.stop()
        loop.close()
