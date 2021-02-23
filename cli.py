"""
Author:     David Walshe
Date:       12 February 2021
"""

# Setup logger config
import src.logger.logger_setup

# Core Modules
import logging

# 3rd Party Modules
import click

# Custom Modules
from src.cli.config import GROUP_CONTEXT_SETTINGS
from src.cli.commands.image.image import image


logger = logging.getLogger(__name__)


@click.group(**GROUP_CONTEXT_SETTINGS)
def cli(**kwargs):
    """
    Base CLI command.
    """
    pass


# ==================================================
# Add CLI groups
# ==================================================
groups = [
    image
]

for group in groups:
    cli.add_command(group)

# ==================================================
# Add CLI commands
# ==================================================
commands = [

]

for command in commands:
    cli.add_command(command)

if __name__ == '__main__':
    cli()
