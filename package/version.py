# fmt: off
"""
A version module managed by the invoke-release task

See also tasks.py
"""

from __future__ import unicode_literals

__version_info__ = (0, 1, 0)
__version__ = '-'.join(filter(None, ['.'.join(map(str, __version_info__[:3])), (__version_info__[3:] or [None])[0]]))
# fmt: on
