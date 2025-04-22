# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import logging
from random import SystemRandom
from typing import Any, Dict

import falcon
from falcon import HTTPBadRequest, HTTPNotFound, HTTPUnauthorized
from falcon.util import uri
from ujson import dumps

# Assuming these modules are correctly set up in the project structure
from oncall import db
from oncall.api.v0.users import get_user_data  # Assuming this function exists

from . import auth_manager  # Assuming this auth manager exists

# Set up logger for this module
log = logging.getLogger(__name__)

# This flag seems unused within this specific function, but kept for broader context
# If it's genuinely unused across the app, it could be removed.
allow_no_auth = True


def on_post(req: falcon.Request, resp: falcon.Response) -> None:
    """
    Handles user login attempts via POST request.

    Expects 'username' and 'password' in the URL-encoded request body.
    Authenticates the user, creates a session, generates a CSRF token,
    stores it, and returns user data along with the CSRF token.
    """
    log.info("Login attempt received.")

    # 1. Parse Credentials
    try:
        # Ensure body exists and can be decoded
        body = req.context.get("body")
        if not body:
            log.warning("Login attempt failed: Request body missing.")
            raise HTTPBadRequest(
                title="Invalid Login Attempt",
                description="Request body missing.",
            )

        login_info = uri.parse_query_string(body.decode("utf-8"))
        user = login_info.get("username")
        password = login_info.get("password")

        if not user or not password:
            log.warning("Login attempt failed: Username or password missing.")
            raise HTTPBadRequest(
                title="Invalid Login Attempt",
                description="Username and password are required.",
            )
        log.info(f"Attempting login for user: {user}")

    except UnicodeDecodeError:
        log.warning(
            "Login attempt failed: Could not decode request body (expected utf-8)."
        )
        raise HTTPBadRequest(
            title="Invalid Request Format",
            description="Request body must be UTF-8 encoded.",
        )
    except Exception as e:
        # Catch unexpected parsing errors
        log.error(
            f"Error parsing login request for user '{user}': {e}", exc_info=True
        )
        raise HTTPBadRequest(
            title="Invalid Request", description="Could not parse login data."
        )

    # 2. Authenticate User
    if not auth_manager.authenticate(user, password):
        log.warning(f"Authentication failed for user: {user}")
        raise HTTPUnauthorized(
            title="Authentication Failure",
            description="Invalid username or password provided.",
            challenges=None,  # Avoid default WWW-Authenticate header if not needed
        )
    log.info(f"User '{user}' authenticated successfully.")

    # 3. Database Operations (Get User Data, Store Session/CSRF)
    try:
        # Use context managers for reliable connection/cursor closing
        with db.connect() as connection:
            with connection.cursor(db.DictCursor) as cursor:
                log.debug(f"Fetching user data for: {user}")
                # Pass cursor/connection tuple if required by get_user_data
                # If get_user_data can manage its own cursor, adjust accordingly
                user_data_list = get_user_data(
                    None, {"name": user}, dbinfo=(connection, cursor)
                )

                if not user_data_list:
                    # This state (auth success but no user data) might indicate an inconsistency
                    log.error(
                        f"Authenticated user '{user}' not found in database."
                    )
                    raise HTTPNotFound(
                        description=f"User data not found for '{user}'."
                    )

                # Assuming get_user_data returns a list, take the first element
                user_data = user_data_list[0]
                log.debug(f"User data found for: {user}")

                # 4. Session Management
                try:
                    session = req.env.get("beaker.session")
                    if not session:
                        log.error(
                            "Beaker session not found in request context. Middleware configured correctly?"
                        )
                        raise falcon.HTTPInternalServerError(
                            title="Session Error",
                            description="Session context unavailable.",
                        )

                    session["user"] = user
                    session.save()
                    session_id = (
                        session.id
                    )  # Use session.id for clarity if available
                    log.info(
                        f"Session saved for user '{user}' with session ID: {session_id}"
                    )
                except Exception as e:
                    log.error(
                        f"Failed to save session for user '{user}': {e}",
                        exc_info=True,
                    )
                    raise falcon.HTTPInternalServerError(
                        title="Session Error",
                        description="Failed to save session.",
                    )

                # 5. CSRF Token Generation and Storage
                csrf_token = (
                    f"{SystemRandom().getrandbits(128):x}"  # Use f-string
                )
                log.debug(f"Generated CSRF token for session ID {session_id}")

                try:
                    cursor.execute(
                        "INSERT INTO `session` (`id`, `csrf_token`) VALUES (%s, %s)",
                        (session_id, csrf_token),
                    )
                    log.info(
                        f"Stored CSRF token for session ID {session_id} in database."
                    )
                except db.IntegrityError:
                    # Likely primary key violation (session ID already exists)
                    log.warning(
                        f"Failed to insert CSRF token for session ID {session_id}: Session already exists in CSRF table."
                    )
                    # Consider if this should be an update instead of an error,
                    # or if the previous session should be invalidated.
                    # Keeping original behavior for now:
                    raise HTTPBadRequest(
                        title="Login Attempt Conflict",
                        description="Session conflict. Please try logging out and logging in again.",
                    )
                except Exception as db_err:
                    log.error(
                        f"Database error storing CSRF token for session {session_id}: {db_err}",
                        exc_info=True,
                    )
                    # Rollback might be needed if connect() doesn't handle it automatically on error
                    # connection.rollback() # Add if necessary
                    raise falcon.HTTPInternalServerError(
                        title="Database Error",
                        description="Failed to store session token.",
                    )

                # Commit transaction after successful insert
                connection.commit()
                log.debug(
                    f"Database transaction committed for session {session_id}."
                )

                # TODO: Implement purging of outdated CSRF tokens (separate process/task)

                # 6. Prepare Response
                user_data["csrf_token"] = csrf_token
                resp.content_type = falcon.MEDIA_JSON
                resp.text = dumps(user_data)
                log.info(
                    f"Login successful for user '{user}'. Response prepared."
                )

    except (HTTPNotFound, HTTPUnauthorized, HTTPBadRequest) as http_err:
        # Re-raise handled HTTP errors directly
        raise http_err

    except db.Error as db_err:
        # Catch database connection or operational errors
        log.error(
            f"Database error during login for user '{user}': {db_err}",
            exc_info=True,
        )
        raise falcon.HTTPInternalServerError(
            title="Database Error",
            description="An unexpected database error occurred.",
        )
    except Exception as e:
        # Catch any other unexpected errors
        log.error(
            f"Unexpected error during login for user '{user}': {e}",
            exc_info=True,
        )
        raise falcon.HTTPInternalServerError(
            title="Internal Server Error",
            description="An unexpected error occurred during login.",
        )
