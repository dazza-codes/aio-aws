"""
Configure the invoke release task
"""

from invoke_release.tasks import *  # noqa: F403
from invoke_release.plugins import PatternReplaceVersionInFilesPlugin

configure_release_parameters(  # noqa: F405
    module_name="package",
    display_name="package",
    # python_directory="package",
    plugins=[
        PatternReplaceVersionInFilesPlugin(
            "docs/conf.py", "package/version.py", "pyproject.toml", "README.md"
        )
    ],
)
