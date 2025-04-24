# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import logging

from falcon import HTTPInternalServerError, HTTPNotFound

from . import db

logger = logging.getLogger(__name__)


class HealthCheck(object):

    def __init__(self, config):
        if config.get("debug") or config.get("auth").get("debug"):
            self.dummy_status = "GOOD"
        else:
            self.dummy_status = None
            path = config.get("healthcheck_path")
            if not path:
                self.dummy_status = "BAD"
            else:
                self.path = path

    def on_get(self, req, resp):
        """
        Health check endpoint. Checks database connectivity and potentially a healthcheck file.
        """
        # Check for dummy status first
        if self.dummy_status:
            status = self.dummy_status
        else:
            # Perform database health check
            try:
                # *** Use the 'with' statement for safe database interaction ***
                with db.connect() as connection:
                    cursor = connection.cursor() # Use standard cursor
                    # Execute a simple query to check connectivity
                    cursor.execute("SELECT VERSION();")
                    # No need to fetch results unless validating version etc.
                    # Just successful execution implies connection worked.

                    # The connection and cursor are automatically closed/released
                    # when the 'with' block exits.
                    # Explicit close calls are no longer needed.

                # If the with block completes without raising an exception, DB check succeeded
                # Continue to file check or set a default success status here if no file check

            except Exception as e: # Catch any exception during database interaction
                logger.exception("Failed to query DB for healthcheck: %s", e) # Log the actual exception
                # Re-raise as HTTPInternalServerError for the caller (Falcon)
                raise HTTPInternalServerError("Database Healthcheck Failed")


            # Perform health check based on file content (if path is set)
            # This part is separate from the database interaction
            if hasattr(self, 'path') and self.path: # Check if path attribute exists and is not None/empty
                try:
                    with open(self.path) as f:
                        status = f.readline().strip()
                except IOError as e: # Catch specific IOError
                    logger.error("Could not open healthcheck file '%s': %s", self.path, e)
                    # Raise HTTPNotFound if the healthcheck file is missing
                    raise HTTPNotFound(description=f"Healthcheck file '{self.path}' not found or readable")
                except Exception as e: # Catch other unexpected errors reading the file
                    logger.error("Unexpected error reading healthcheck file '%s': %s", self.path, e)
                    raise HTTPInternalServerError("Error reading healthcheck file")
            else:
                # If no dummy status and no healthcheck file path, assume DB success means OK
                status = "OK" # Or a more specific success message


        # Set response headers and body
        resp.content_type = "text/plain"
        resp.text = status
        # Default Falcon status is 200 OK if not set otherwise


def init(application, config):
    application.add_route("/healthcheck", HealthCheck(config))
