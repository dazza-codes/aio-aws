"""
Test the notes.concurrency_async module
"""
from types import ModuleType

from notes import concurrency_async


def test_concurrency_module():
    assert isinstance(concurrency_async, ModuleType)
