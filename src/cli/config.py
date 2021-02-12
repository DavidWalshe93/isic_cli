"""
Author:     David Walshe
Date:       12 February 2021
"""

# ===========================================================
# Click Context Options
# ===========================================================
COMMAND_CONTEXT_SETTINGS = {
    "context_settings": {
        "help_option_names": ["-h", "--help"]
    }
}

GROUP_CONTEXT_SETTINGS = {
    **COMMAND_CONTEXT_SETTINGS,
    "invoke_without_command": True
}