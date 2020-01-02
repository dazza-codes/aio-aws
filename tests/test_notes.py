"""
Test the notes package
"""
from types import ModuleType

import notes


def test_notes_package():
    assert isinstance(notes, ModuleType)
