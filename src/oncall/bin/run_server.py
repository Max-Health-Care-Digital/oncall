#!/usr/bin/env python

# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# --- GEVENT PATCHING ---
# This MUST be absolutely first, before any other imports that might import ssl, socket, etc.
import gevent.monkey

gevent.monkey.patch_all()
# --- END GEVENT PATCHING ---

import argparse
import logging
import multiprocessing
import sys
from typing import Any, Dict, Optional

import gunicorn.app.base

# Assuming oncall and its submodules exist relative to the execution path
# or are installed in the environment.
# It's generally better practice to ensure 'oncall' is a proper package.
import oncall.app
import oncall.ui
import oncall.utils

# Set up basic logging for the script itself
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


class StandaloneApplication(gunicorn.app.base.BaseApplication):
    """
    Gunicorn application class for running the Oncall application standalone.

    Integrates Oncall with Gunicorn, handling configuration loading and
    optional asset building via Gunicorn hooks.
    """

    def __init__(
        self, app_config: Dict[str, Any], skip_build_assets: bool = False
    ):
        """
        Initialize the Gunicorn application.

        Args:
            app_config: The application configuration dictionary loaded from the config file.
            skip_build_assets: Flag to indicate whether to skip building UI assets.
        """
        self.app_config = app_config
        self.options = self._build_gunicorn_options(app_config)
        self.skip_build_assets = skip_build_assets
        # The Gunicorn base class __init__ must be called last
        # as it processes the 'config' attribute if it exists,
        # which we don't explicitly set here, relying instead on load_config.
        super().__init__()

    def _build_gunicorn_options(
        self, app_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Builds the Gunicorn options dictionary from the app config."""
        try:
            server_cfg = app_config["server"]
            host = server_cfg.get("host", "127.0.0.1")
            port = server_cfg.get("port", 8080)
        except KeyError:
            log.error("Config file is missing 'server' section.")
            sys.exit(1)
        except TypeError as e:
            log.error(f"Invalid format in 'server' section of config file: {e}")
            sys.exit(1)

        # Default Gunicorn options
        options = {
            "preload_app": False,  # Important for when_ready hook and zero-downtime restarts
            "reload": False,  # Typically False for production, True for development
            "bind": f"{host}:{port}",
            "worker_class": "gevent",
            "accesslog": "-",
            "errorlog": "-",
            "workers": multiprocessing.cpu_count(),
            "when_ready": self.when_ready,  # Use the Gunicorn server hook
        }

        # Allow overriding Gunicorn settings via a 'gunicorn' section in the config
        gunicorn_overrides = app_config.get("gunicorn", {})
        options.update(gunicorn_overrides)

        # Ensure essential options derived from 'server' config take precedence
        options["bind"] = f"{host}:{port}"

        return options

    def load_config(self) -> None:
        """Loads Gunicorn configuration settings from the prepared options."""
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            # Gunicorn settings are lowercase
            self.cfg.set(key.lower(), value)

    def load(self) -> Any:
        """Loads the WSGI application."""
        # No need for importlib.reload in a typical deployment scenario.
        # Gunicorn's reload mechanism handles code changes if enabled.
        log.info("Loading Oncall WSGI application.")
        return oncall.app.get_wsgi_app()

    def when_ready(self, server: Any) -> None:
        """
        Gunicorn server hook executed in the master process before workers fork.

        Used here to build static assets exactly once.
        """
        log.info("Gunicorn master process ready.")
        if not self.skip_build_assets:
            log.info("Building Oncall UI assets...")
            try:
                oncall.ui.build_assets()
                log.info("Successfully built Oncall UI assets.")
            except Exception as e:
                log.error(
                    f"Failed to build Oncall UI assets: {e}", exc_info=True
                )
                # Decide if this is a fatal error
                # server.halt("Asset building failed.") # Uncomment to stop server on failure
        else:
            log.info("Skipping asset building as requested.")


def main() -> None:
    """Parses arguments, loads configuration, and runs the Gunicorn server."""
    parser = argparse.ArgumentParser(
        description="Run the Oncall application using Gunicorn."
    )
    parser.add_argument(
        "config_file",
        help="Path to the Oncall configuration file (e.g., config.yaml).",
    )
    parser.add_argument(
        "--skip-build-assets",
        action="store_true",
        help="Skip the UI asset building step.",
    )
    args = parser.parse_args()

    try:
        log.info(f"Loading configuration from: {args.config_file}")
        config = oncall.utils.read_config(args.config_file)
    except FileNotFoundError:
        log.error(f"Configuration file not found: {args.config_file}")
        sys.exit(1)
    except Exception as e:
        # Catch other potential errors from read_config (e.g., YAML parsing)
        log.error(f"Error reading configuration file {args.config_file}: {e}")
        sys.exit(1)

    if not isinstance(config, dict):
        log.error(
            f"Configuration file {args.config_file} did not load as a dictionary."
        )
        sys.exit(1)

    log.info("Initializing Gunicorn server...")
    # Pass the whole config dictionary and the skip flag
    gunicorn_server = StandaloneApplication(config, args.skip_build_assets)
    gunicorn_server.run()


if __name__ == "__main__":
    main()
