"""
Configure the invoke release task
"""

from invoke_release.tasks import *  # noqa: F403
from invoke_release.plugins import PatternReplaceVersionInFilesPlugin

configure_release_parameters(  # noqa: F405
    module_name="notes",
    display_name="python-notes",
    # python_directory="package",
    plugins=[
        PatternReplaceVersionInFilesPlugin(
            "docs/conf.py", "notes/version.py", "pyproject.toml", "README.md"
        )
    ],
)
