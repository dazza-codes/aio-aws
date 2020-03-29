"""
Configure the invoke release task
"""

from invoke_release.plugins import PatternReplaceVersionInFilesPlugin
from invoke_release.tasks import *  # noqa: F403

configure_release_parameters(  # noqa: F405
    module_name="aio_aws",
    display_name="aio-aws",
    # python_directory="package",
    plugins=[
        PatternReplaceVersionInFilesPlugin(
            "docs/conf.py", "aio_aws/version.py", "pyproject.toml", "README.md"
        )
    ],
)
