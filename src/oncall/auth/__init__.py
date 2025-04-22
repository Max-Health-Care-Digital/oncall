# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import base64
import hashlib
import hmac
import importlib
import logging
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union
from urllib.parse import quote

import falcon
from falcon import HTTPForbidden, HTTPUnauthorized, Request

# Assuming 'db' is correctly configured and provides connect/cursor methods
# and appropriate exception types (like db.Error)
from .. import db

# Logger instance already set up
logger = logging.getLogger("oncall.auth")

# Module-level globals for auth managers, initialized later in init()
# Using typing.Any for now, replace with specific class types if available
auth_manager: Optional[Any] = None
sso_auth_manager: Optional[Any] = None

# Type alias for database cursors if specific types are known, otherwise Any
DbCursor = Any  # Replace with Type[db.DictCursor] or similar if applicable


# =============================================================================
# Helper Functions & Decorators (Initial Definitions)
# =============================================================================


def _debug_only_forbidden(function: Callable) -> Callable:
    """Decorator that raises HTTPForbidden. Replaced in debug mode."""

    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        logger.warning(
            f"Attempted to call admin-only function {function.__name__} while not in debug mode."
        )
        raise HTTPForbidden(
            title="Admin Only", description="This action is restricted."
        )

    return wrapper


# Initially, debug_only blocks execution
debug_only = _debug_only_forbidden


def is_god(challenger: str) -> bool:
    """
    Checks if the given user has the 'god' flag set in the database.

    Args:
        challenger: The username to check.

    Returns:
        True if the user is a god user, False otherwise.
    """
    logger.debug(f"Checking god status for user: {challenger}")
    try:
        with db.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT `id` FROM `user` WHERE `god` = TRUE AND `name` = %s LIMIT 1",
                    (challenger,),
                )
                is_god_result = cursor.rowcount > 0
                log_msg = "is" if is_god_result else "is not"
                logger.debug(f"User '{challenger}' {log_msg} a god user.")
                return is_god_result
    except db.Error as e:
        logger.error(
            f"Database error checking god status for '{challenger}': {e}",
            exc_info=True,
        )
        # Depending on policy, might want to default to False or raise an error
        return False
    except Exception as e:
        logger.error(
            f"Unexpected error checking god status for '{challenger}': {e}",
            exc_info=True,
        )
        return False


def check_ical_key_admin(challenger: str) -> bool:
    """
    Checks if a user is authorized to administer iCal keys (currently only god users).

    Args:
        challenger: The username attempting the action.

    Returns:
        True if authorized, False otherwise. (Note: Original didn't raise, just returned bool)
    """
    # This function didn't raise Forbidden in the original, just returned bool. Preserving that.
    is_admin = is_god(challenger)
    logger.debug(
        f"iCal key admin check for '{challenger}': {'Authorized (god)' if is_admin else 'Not authorized'}"
    )
    return is_admin


# =============================================================================
# Authorization Check Functions (Initial Definitions)
# These may be replaced by lambda functions in debug mode via init()
# =============================================================================


def _check_user_auth_impl(target_user: str, req: Request) -> None:
    """
    Checks if the current user (challenger) is authorized to act on behalf of the target_user.
    Authorization criteria:
    1. Challenger is the target user.
    2. Challenger is an admin of a team the target user belongs to.
    3. Challenger is a god user.
    Bypassed if request is authenticated as an application.

    Args:
        target_user: The user being acted upon.
        req: The Falcon request object.

    Raises:
        HTTPForbidden: If authorization fails.
    """
    if "app" in req.context:
        logger.debug(
            f"User auth check skipped for target '{target_user}' (request authenticated as app '{req.context.get('app')}')."
        )
        return

    challenger = req.context.get("user")
    if not challenger:
        logger.warning(
            "User auth check failed: No challenger user found in request context."
        )
        raise HTTPForbidden(
            title="Authorization Error",
            description="Requesting user context not found.",
        )

    logger.debug(
        f"Auth check: Challenger '{challenger}' acting on target user '{target_user}'."
    )

    if target_user == challenger:
        logger.debug(
            f"Auth check passed: Challenger '{challenger}' matches target user."
        )
        return

    if is_god(challenger):
        logger.debug(
            f"Auth check passed: Challenger '{challenger}' is a god user."
        )
        return

    try:
        with db.connect() as connection:
            with connection.cursor() as cursor:
                # Check if challenger is admin of any team target_user is in
                get_allowed_query = """
                    SELECT 1
                    FROM `team_admin` ta
                    JOIN `team_user` tu ON ta.`team_id` = tu.`team_id`
                    JOIN `user` target_u ON target_u.`id` = tu.`user_id`
                    JOIN `user` admin_u ON admin_u.`id` = ta.`user_id`
                    WHERE admin_u.`name` = %s AND target_u.`name` = %s
                    LIMIT 1
                """
                cursor.execute(get_allowed_query, (challenger, target_user))
                is_admin_of_user_team = cursor.rowcount > 0

                if is_admin_of_user_team:
                    logger.debug(
                        f"Auth check passed: Challenger '{challenger}' is admin of a team containing target user '{target_user}'."
                    )
                    return

    except db.Error as e:
        logger.error(
            f"Database error during user auth check for challenger '{challenger}' on target '{target_user}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Database error during permission check.",
        )
    except Exception as e:
        logger.error(
            f"Unexpected error during user auth check for challenger '{challenger}' on target '{target_user}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Unexpected error during permission check.",
        )

    # If none of the conditions met
    logger.warning(
        f"Auth check FAILED: Challenger '{challenger}' not authorized for target user '{target_user}'."
    )
    raise HTTPForbidden(
        title="Unauthorized",
        description=f"Action not allowed for user '{challenger}' on target '{target_user}'.",
    )


check_user_auth = _check_user_auth_impl


def _check_team_auth_impl(team_name: str, req: Request) -> None:
    """
    Checks if the current user (challenger) is an admin of the specified team.
    Authorization criteria:
    1. Challenger is an admin of the team.
    2. Challenger is a god user.
    Bypassed if request is authenticated as an application.

    Args:
        team_name: The name of the team being acted upon.
        req: The Falcon request object.

    Raises:
        HTTPForbidden: If authorization fails.
    """
    if "app" in req.context:
        logger.debug(
            f"Team auth check skipped for team '{team_name}' (request authenticated as app '{req.context.get('app')}')."
        )
        return

    challenger = req.context.get("user")
    if not challenger:
        logger.warning(
            "Team auth check failed: No challenger user found in request context."
        )
        raise HTTPForbidden(
            title="Authorization Error",
            description="Requesting user context not found.",
        )

    logger.debug(
        f"Auth check: Challenger '{challenger}' acting on team '{team_name}'."
    )

    if is_god(challenger):
        logger.debug(
            f"Team auth check passed: Challenger '{challenger}' is a god user."
        )
        return

    try:
        with db.connect() as connection:
            with connection.cursor() as cursor:
                get_allowed_query = """
                    SELECT 1
                    FROM `team_admin` ta
                    JOIN `team` t ON ta.`team_id` = t.`id`
                    JOIN `user` u ON ta.`user_id` = u.`id`
                    WHERE u.`name` = %s AND t.`name` = %s
                    LIMIT 1
                """
                cursor.execute(get_allowed_query, (challenger, team_name))
                is_team_admin = cursor.rowcount > 0

                if is_team_admin:
                    logger.debug(
                        f"Team auth check passed: Challenger '{challenger}' is admin for team '{team_name}'."
                    )
                    return

    except db.Error as e:
        logger.error(
            f"Database error during team auth check for challenger '{challenger}' on team '{team_name}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Database error during permission check.",
        )
    except Exception as e:
        logger.error(
            f"Unexpected error during team auth check for challenger '{challenger}' on team '{team_name}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Unexpected error during permission check.",
        )

    # If none of the conditions met
    logger.warning(
        f"Auth check FAILED: Challenger '{challenger}' not authorized for team '{team_name}'."
    )
    raise HTTPForbidden(
        title="Unauthorized",
        description=f'Action not allowed: User "{challenger}" is not an admin for team "{team_name}".',
    )


check_team_auth = _check_team_auth_impl


def _check_calendar_auth_impl(
    team_name: str, req: Request, user: Optional[str] = None
) -> None:
    """
    Checks if the relevant user (challenger) is a member of the specified team (for calendar access).
    Authorization criteria:
    1. Challenger is a member of the team.
    2. Challenger is a god user.
    Bypassed if request is authenticated as an application.

    Args:
        team_name: The name of the team whose calendar is being accessed.
        req: The Falcon request object.
        user: Optional specific user to check; defaults to the user in request context.

    Raises:
        HTTPForbidden: If authorization fails.
    """
    if "app" in req.context:
        logger.debug(
            f"Calendar auth check skipped for team '{team_name}' (request authenticated as app '{req.context.get('app')}')."
        )
        return

    challenger = user if user is not None else req.context.get("user")
    if not challenger:
        logger.warning(
            "Calendar auth check failed: No challenger user could be determined."
        )
        raise HTTPForbidden(
            title="Authorization Error",
            description="Requesting user context not found.",
        )

    logger.debug(
        f"Calendar auth check: Challenger '{challenger}' accessing calendar for team '{team_name}'."
    )

    if is_god(challenger):
        logger.debug(
            f"Calendar auth check passed: Challenger '{challenger}' is a god user."
        )
        return

    try:
        with db.connect() as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT 1
                    FROM `team_user` tu
                    JOIN `user` u ON tu.`user_id` = u.`id`
                    JOIN `team` t ON tu.`team_id` = t.`id`
                    WHERE t.`name` = %s AND u.`name` = %s
                    LIMIT 1
                """
                cursor.execute(query, (team_name, challenger))
                is_team_member = cursor.rowcount > 0

                if is_team_member:
                    logger.debug(
                        f"Calendar auth check passed: Challenger '{challenger}' is member of team '{team_name}'."
                    )
                    return

    except db.Error as e:
        logger.error(
            f"Database error during calendar auth check for challenger '{challenger}' on team '{team_name}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Database error during permission check.",
        )
    except Exception as e:
        logger.error(
            f"Unexpected error during calendar auth check for challenger '{challenger}' on team '{team_name}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Unexpected error during permission check.",
        )

    # If none of the conditions met
    logger.warning(
        f"Auth check FAILED: Challenger '{challenger}' not authorized for calendar of team '{team_name}'."
    )
    raise HTTPForbidden(
        title="Unauthorized",
        description=f'Action not allowed: User "{challenger}" is not part of team "{team_name}".',
    )


check_calendar_auth = _check_calendar_auth_impl


def _check_calendar_auth_by_id_impl(
    team_id: Union[int, str], req: Request
) -> None:
    """
    Checks if the current user (challenger) is a member of the specified team ID (for calendar access).
    Authorization criteria:
    1. Challenger is a member of the team.
    2. Challenger is a god user.
    Bypassed if request is authenticated as an application.

    Args:
        team_id: The ID of the team whose calendar is being accessed.
        req: The Falcon request object.

    Raises:
        HTTPForbidden: If authorization fails.
    """
    if "app" in req.context:
        logger.debug(
            f"Calendar auth check (by ID) skipped for team ID '{team_id}' (request authenticated as app '{req.context.get('app')}')."
        )
        return

    challenger = req.context.get("user")
    if not challenger:
        logger.warning(
            "Calendar auth check (by ID) failed: No challenger user found in request context."
        )
        raise HTTPForbidden(
            title="Authorization Error",
            description="Requesting user context not found.",
        )

    logger.debug(
        f"Calendar auth check (by ID): Challenger '{challenger}' accessing calendar for team ID '{team_id}'."
    )

    if is_god(challenger):
        logger.debug(
            f"Calendar auth check (by ID) passed: Challenger '{challenger}' is a god user."
        )
        return

    try:
        with db.connect() as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT 1
                    FROM `team_user` tu
                    JOIN `user` u ON tu.`user_id` = u.`id`
                    WHERE tu.`team_id` = %s AND u.`name` = %s
                    LIMIT 1
                """
                cursor.execute(query, (team_id, challenger))
                is_team_member = cursor.rowcount > 0

                if is_team_member:
                    logger.debug(
                        f"Calendar auth check (by ID) passed: Challenger '{challenger}' is member of team ID '{team_id}'."
                    )
                    return

    except db.Error as e:
        logger.error(
            f"Database error during calendar auth check (by ID) for challenger '{challenger}' on team ID '{team_id}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Database error during permission check.",
        )
    except Exception as e:
        logger.error(
            f"Unexpected error during calendar auth check (by ID) for challenger '{challenger}' on team ID '{team_id}': {e}",
            exc_info=True,
        )
        raise HTTPForbidden(
            title="Authorization Check Failed",
            description="Unexpected error during permission check.",
        )

    # If none of the conditions met
    logger.warning(
        f"Auth check FAILED: Challenger '{challenger}' not authorized for calendar of team ID '{team_id}'."
    )
    raise HTTPForbidden(
        title="Unauthorized",
        description=f'Action not allowed: User "{challenger}" is not a member of the specified team.',
    )


check_calendar_auth_by_id = _check_calendar_auth_by_id_impl


# =============================================================================
# Application (HMAC) Authentication
# =============================================================================


def is_client_digest_valid(
    client_digest: str,
    api_key: bytes,
    window: int,
    method: str,
    path: str,
    body: str,
) -> bool:
    """
    Calculates expected HMAC digests and compares with the client-provided digest.
    Checks both quoted and unquoted paths for compatibility. Uses constant-time comparison.

    Args:
        client_digest: The base64 encoded digest provided by the client.
        api_key: The secret API key (as bytes).
        window: The time window value.
        method: The HTTP method.
        path: The request path (including query string).
        body: The request body (as string).

    Returns:
        True if the client digest matches either calculated digest, False otherwise.
    """
    try:
        client_digest_bytes = client_digest.encode("utf-8")

        # Calculate with quoted path
        text_quoted = f"{window} {method} {quote(path)} {body}".encode("utf-8")
        hmac_quoted = hmac.new(api_key, text_quoted, hashlib.sha512)
        digest_quoted = base64.urlsafe_b64encode(hmac_quoted.digest())
        logger.debug(
            f"Calculated HMAC (quoted path, window {window}): {digest_quoted.decode('utf-8')}"
        )

        if hmac.compare_digest(client_digest_bytes, digest_quoted):
            logger.debug("Client digest matches HMAC with quoted path.")
            return True

        # Calculate with unquoted path (for legacy compatibility)
        text_unquoted = f"{window} {method} {path} {body}".encode("utf-8")
        hmac_unquoted = hmac.new(api_key, text_unquoted, hashlib.sha512)
        digest_unquoted = base64.urlsafe_b64encode(hmac_unquoted.digest())
        logger.debug(
            f"Calculated HMAC (unquoted path, window {window}): {digest_unquoted.decode('utf-8')}"
        )

        if hmac.compare_digest(client_digest_bytes, digest_unquoted):
            logger.debug(
                "Client digest matches HMAC with unquoted path (legacy)."
            )
            return True

        logger.debug(
            f"Client digest '{client_digest}' did not match calculated digests for window {window}."
        )
        return False
    except Exception as e:
        logger.error(
            f"Error during HMAC calculation or comparison: {e}", exc_info=True
        )
        return False


def authenticate_application(auth_token: str, req: Request) -> None:
    """
    Authenticates a request based on an HMAC Authorization header.
    Validates the HMAC digest against calculated values for different time windows.
    Sets req.context['app'] if authentication succeeds.

    Args:
        auth_token: The value of the Authorization header.
        req: The Falcon request object.

    Raises:
        HTTPUnauthorized: If authentication fails at any stage.
    """
    logger.debug("Attempting application authentication via HMAC header.")
    if not auth_token or not auth_token.lower().startswith("hmac "):
        logger.warning(
            "Application auth failed: Header missing or invalid format."
        )
        raise HTTPUnauthorized(
            title="Authentication Failure",
            description="Invalid or missing HMAC Authorization header format.",
        )

    method = req.method
    path = req.path  # req.path includes query string already in Falcon
    # Original used req.env['PATH_INFO'] and req.env['QUERY_STRING'], req.path is generally preferred
    # path = req.env["PATH_INFO"]
    # qs = req.env["QUERY_STRING"]
    # if qs:
    #     path = path + "?" + qs

    try:
        # Decode body carefully
        body_bytes = req.context.get("body")
        body = body_bytes.decode("utf-8") if body_bytes else ""
    except UnicodeDecodeError:
        logger.warning(
            "Application auth failed: Request body is not valid UTF-8."
        )
        raise HTTPUnauthorized(
            title="Authentication Failure",
            description="Invalid request body encoding.",
        )
    except Exception as e:
        logger.error(
            f"Application auth failed: Error accessing request body: {e}",
            exc_info=True,
        )
        raise HTTPUnauthorized(
            title="Authentication Failure",
            description="Failed to process request body.",
        )

    try:
        # Parse "hmac app_name:client_digest"
        auth_parts = auth_token[5:].split(":", 1)
        if len(auth_parts) != 2:
            raise ValueError("Auth token format invalid")
        app_name, client_digest = auth_parts
        logger.debug(
            f"Received HMAC token for app: '{app_name}', digest: '{client_digest[:10]}...'"
        )

        api_key: Optional[bytes] = None
        try:
            with db.connect() as connection:
                with connection.cursor() as cursor:
                    logger.debug(
                        f"Looking up API key for application: {app_name}"
                    )
                    cursor.execute(
                        "SELECT `key` FROM `application` WHERE `name` = %s LIMIT 1",
                        (app_name,),
                    )
                    if cursor.rowcount > 0:
                        api_key_str = cursor.fetchone()[0]
                        api_key = api_key_str.encode("utf-8")
                        logger.debug(
                            f"API key found for application: {app_name}"
                        )
                    else:
                        logger.warning(
                            f"Application auth failed: Application '{app_name}' not found in database."
                        )
                        raise HTTPUnauthorized(
                            title="Authentication Failure",
                            description=f"Application '{app_name}' not found.",
                        )
        except db.Error as e:
            logger.error(
                f"Database error looking up API key for app '{app_name}': {e}",
                exc_info=True,
            )
            raise HTTPUnauthorized(
                title="Authentication Failure",
                description="Database error during authentication.",
            )

        # Check time windows
        current_time = int(time.time())
        window_short = current_time // 5
        window_long = current_time // 30

        windows_to_check = [
            window_short,  # Current short window
            window_short - 1,  # Previous short window
            window_long,  # Current long window
            window_long - 1,  # Previous long window
        ]

        for window in windows_to_check:
            logger.debug(f"Checking digest against window: {window}")
            if is_client_digest_valid(
                client_digest, api_key, window, method, path, body
            ):
                logger.info(
                    f"Application authentication successful for app: '{app_name}' using window {window}."
                )
                req.context["app"] = app_name
                return  # Authentication successful

        # If loop completes without returning, all checks failed
        logger.warning(
            f"Application auth failed: Client digest for app '{app_name}' did not match any valid window."
        )
        raise HTTPUnauthorized(
            title="Authentication Failure", description="Invalid HMAC digest."
        )

    except (ValueError, KeyError) as e:
        logger.warning(
            f"Application auth failed: Error parsing HMAC token '{auth_token[:20]}...': {e}"
        )
        raise HTTPUnauthorized(
            title="Authentication Failure",
            description="Invalid HMAC token format.",
        )
    except HTTPUnauthorized:
        # Re-raise specific auth errors
        raise
    except Exception as e:
        # Catch any other unexpected errors during the process
        logger.error(
            f"Unexpected error during application authentication for token '{auth_token[:20]}...': {e}",
            exc_info=True,
        )
        raise HTTPUnauthorized(
            title="Authentication Failure",
            description="An unexpected error occurred during authentication.",
        )


# =============================================================================
# User (SSO/Session/CSRF) Authentication
# =============================================================================


def _authenticate_user_impl(req: Request) -> None:
    """
    Authenticates a user based on SSO headers (if configured) or session/CSRF tokens.
    Sets req.context['user'] if authentication succeeds.

    Args:
        req: The Falcon request object.

    Raises:
        HTTPUnauthorized: If authentication fails.
    """
    global sso_auth_manager
    logger.debug("Attempting user authentication.")

    # 1. Try SSO Authentication first (if manager exists)
    if sso_auth_manager:
        logger.debug("Checking SSO authentication manager.")
        try:
            sso_user = sso_auth_manager.authenticate(req)
            if sso_user:
                logger.info(
                    f"User authentication successful via SSO for user: '{sso_user}'."
                )
                req.context["user"] = sso_user
                return  # SSO auth successful
            else:
                logger.debug(
                    "SSO authentication manager did not authenticate user, falling back to session."
                )
        except Exception as e:
            logger.error(
                f"Error during SSO authentication check: {e}", exc_info=True
            )
            # Decide policy: fail open (fall through) or fail closed (raise)?
            # Original falls through, so preserving that.

    # 2. Fallback to Session/CSRF Authentication
    logger.debug("Attempting session/CSRF authentication.")
    session = req.env.get("beaker.session")  # Use .get for safer access
    if not session:
        logger.warning(
            "User auth failed: Beaker session not found in request context."
        )
        raise HTTPUnauthorized(
            title="Unauthorized", description="User session not found."
        )

    try:
        user = session.get("user")
        session_id = session.id  # Or session['_id'] if .id not available
        if not user or not session_id:
            logger.warning(
                f"User auth failed: 'user' or 'id' missing from session object. Session ID: {session_id}"
            )
            raise HTTPUnauthorized(
                title="Invalid Session", description="Incomplete session data."
            )

        logger.debug(
            f"Session found for user '{user}' with ID '{session_id}'. Verifying CSRF token."
        )

        # Verify CSRF token
        csrf_header = req.get_header("X-CSRF-TOKEN")
        if not csrf_header:
            logger.warning(
                f"User auth failed: X-CSRF-TOKEN header missing for user '{user}', session '{session_id}'."
            )
            raise HTTPUnauthorized(
                title="Invalid Session",
                description="Missing CSRF token in request header.",
            )

        try:
            with db.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT `csrf_token` FROM `session` WHERE `id` = %s LIMIT 1",
                        (session_id,),
                    )
                    result = cursor.fetchone()
                    if not result:
                        logger.warning(
                            f"User auth failed: No CSRF token found in database for session ID '{session_id}' (user '{user}')."
                        )
                        raise HTTPUnauthorized(
                            title="Invalid Session",
                            description="Session token not found or expired.",
                        )

                    stored_token = result[0]
                    # Use constant time comparison
                    if hmac.compare_digest(csrf_header, stored_token):
                        logger.info(
                            f"User authentication successful via session/CSRF for user: '{user}'."
                        )
                        req.context["user"] = user
                        return  # Session/CSRF auth successful
                    else:
                        logger.warning(
                            f"User auth failed: CSRF token mismatch for user '{user}', session '{session_id}'. Header: '{csrf_header[:10]}...', Stored: '{stored_token[:10]}...'"
                        )
                        raise HTTPUnauthorized(
                            title="Invalid Session",
                            description="CSRF token validation failed.",
                        )

        except db.Error as e:
            logger.error(
                f"Database error verifying CSRF token for user '{user}', session '{session_id}': {e}",
                exc_info=True,
            )
            raise HTTPUnauthorized(
                title="Authentication Error",
                description="Database error during CSRF validation.",
            )

    except KeyError as e:
        logger.warning(
            f"User auth failed: Missing expected key in session object: {e}. Session keys: {list(session.keys())}"
        )
        raise HTTPUnauthorized(
            title="Invalid Session",
            description=f"Missing expected data in session: {e}.",
        )
    except HTTPUnauthorized:
        # Re-raise specific auth errors
        raise
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(
            f"Unexpected error during session/CSRF authentication: {e}",
            exc_info=True,
        )
        raise HTTPUnauthorized(
            title="Authentication Error",
            description="An unexpected error occurred during user authentication.",
        )


# Assign the implementation to the potentially-overridden name
authenticate_user = _authenticate_user_impl


# =============================================================================
# Login Required Decorator (Initial Definition)
# =============================================================================


def _login_required_impl(function: Callable) -> Callable:
    """
    Decorator for Falcon resource methods requiring authentication.
    Checks for application (HMAC) or user (SSO/Session) authentication.
    """

    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Find the Request object in args (usually the second arg in on_get, on_post etc.)
        req = None
        for arg in args:
            if isinstance(arg, Request):
                req = arg
                break
        if not req:
            # This should realistically not happen if decorating a falcon method
            logger.error(
                f"Auth decorator failed: Could not find Falcon Request object in arguments for {function.__name__}."
            )
            raise falcon.HTTPInternalServerError(
                title="Configuration Error",
                description="Auth decorator applied incorrectly.",
            )

        # Check for Application Authentication first
        auth_token = req.get_header("AUTHORIZATION")
        if auth_token:
            logger.debug(
                f"AUTHORIZATION header found for {function.__name__}, attempting application auth."
            )
            try:
                authenticate_application(auth_token, req)
                # If app auth succeeds, proceed to the function
                logger.debug(
                    f"Application auth successful for {function.__name__}, proceeding."
                )
                return function(*args, **kwargs)
            except HTTPUnauthorized as e:
                # If app auth fails, re-raise the specific error
                logger.warning(
                    f"Application auth failed for {function.__name__}: {e.title} - {e.description}"
                )
                raise e
            except Exception as e:
                logger.error(
                    f"Unexpected error during application auth check in decorator for {function.__name__}: {e}",
                    exc_info=True,
                )
                raise HTTPUnauthorized(
                    title="Authentication Error",
                    description="Unexpected error during application authentication.",
                )

        else:
            # No Authorization header, attempt User Authentication
            logger.debug(
                f"No AUTHORIZATION header for {function.__name__}, attempting user auth."
            )
            try:
                authenticate_user(
                    req
                )  # authenticate_user might be the original or debug version
                # If user auth succeeds, proceed to the function
                logger.debug(
                    f"User auth successful for {function.__name__}, proceeding."
                )
                return function(*args, **kwargs)
            except HTTPUnauthorized as e:
                # If user auth fails, re-raise the specific error
                logger.warning(
                    f"User auth failed for {function.__name__}: {e.title} - {e.description}"
                )
                raise e
            except Exception as e:
                logger.error(
                    f"Unexpected error during user auth check in decorator for {function.__name__}: {e}",
                    exc_info=True,
                )
                raise HTTPUnauthorized(
                    title="Authentication Error",
                    description="Unexpected error during user authentication.",
                )

    return wrapper


login_required = _login_required_impl


# =============================================================================
# Initialization Function
# =============================================================================

# Define types for the functions that might be replaced
AuthCheckFunc = Callable[[str, Request], None]
AuthCheckByIdFunc = Callable[[Union[str, int], Request], None]
UserAuthFunc = Callable[[Request], None]
DecoratorFunc = Callable[[Callable], Callable]


def init(application: falcon.App, config: Dict[str, Any]) -> None:
    """
    Initializes the authentication module based on the provided configuration.
    - Sets up SSO authentication manager if configured.
    - Overrides authentication/authorization checks if debug mode is enabled.
    - Overrides the login_required decorator if docs/require_auth modes are enabled.
    - Sets up the primary authentication manager.
    - Adds login/logout routes.

    Args:
        application: The Falcon application instance.
        config: The application configuration dictionary.
    """
    global auth_manager, sso_auth_manager
    # Use Type variables for functions that get reassigned
    global check_team_auth, check_user_auth, check_calendar_auth
    global check_calendar_auth_by_id, debug_only
    global authenticate_user, login_required

    logger.info("Initializing authentication module...")

    # Configure SSO Auth Manager
    sso_module_name = config.get("sso_module")
    if sso_module_name:
        try:
            logger.info(f"Loading SSO module: {sso_module_name}")
            sso_auth_module = importlib.import_module(sso_module_name)
            # Assuming the module has an 'Authenticator' class
            sso_auth_manager = getattr(sso_auth_module, "Authenticator")(config)
            logger.info(
                f"SSO authentication manager configured from {sso_module_name}."
            )
        except ImportError:
            logger.error(
                f"Failed to import SSO module: {sso_module_name}", exc_info=True
            )
            # Decide if this is fatal? Original seems to continue.
        except AttributeError:
            logger.error(
                f"SSO module {sso_module_name} does not have an 'Authenticator' attribute.",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize SSO authenticator from {sso_module_name}: {e}",
                exc_info=True,
            )

    # Configure Debug Mode (Bypasses Auth Checks)
    if config.get("debug", False):
        logger.warning(
            "Auth debug mode ENABLED. Authentication and authorization checks will be bypassed."
        )

        # Wrapper for user auth in debug mode (tries real auth, falls back to test_user)
        def authenticate_user_debug_wrapper(req: Request) -> None:
            try:
                _authenticate_user_impl(req)
                logger.debug("Debug mode: Real user authentication succeeded.")
            except HTTPUnauthorized:
                logger.warning(
                    "Debug mode: Real user authentication failed/skipped, setting user to 'test_user'."
                )
                req.context["user"] = "test_user"  # Avoid login for e2e tests

        authenticate_user = authenticate_user_debug_wrapper
        # Replace check functions with no-ops
        check_team_auth = lambda team_name, req: logger.debug(
            f"[DEBUG MODE] Bypassing team auth check for team: {team_name}"
        )
        check_user_auth = lambda target_user, req: logger.debug(
            f"[DEBUG MODE] Bypassing user auth check for target: {target_user}"
        )
        check_calendar_auth = lambda team_name, req, **kwargs: logger.debug(
            f"[DEBUG MODE] Bypassing calendar auth check for team: {team_name}"
        )
        check_calendar_auth_by_id = lambda team_id, req: logger.debug(
            f"[DEBUG MODE] Bypassing calendar auth check (by ID) for team: {team_id}"
        )
        # Allow debug_only functions to run
        debug_only = lambda function: function
        logger.warning(
            "Debug mode: check_*_auth functions replaced with no-ops."
        )
        logger.warning(
            "Debug mode: authenticate_user replaced with debug wrapper (fallback to 'test_user')."
        )
        logger.warning(
            "Debug mode: @debug_only decorator replaced with identity function."
        )
    else:
        logger.info("Auth debug mode disabled.")

    # Configure Decorator Replacement (Docs/RequireAuth)
    if config.get("docs") or config.get("require_auth"):
        reason = (
            "docs generation" if config.get("docs") else "'require_auth' config"
        )
        logger.info(
            f"Replacing @login_required decorator with identity function due to {reason}."
        )
        # Replace decorator with identity function (pass-through)
        login_required = lambda x: x
    else:
        # If not replaced, load the primary auth manager (used for password login)
        # This is only strictly needed if password login (/login route) is used AND require_auth is False
        auth_module_name = config.get("module")
        if auth_module_name:
            try:
                logger.info(
                    f"Loading primary authentication module: {auth_module_name}"
                )
                auth_module = importlib.import_module(auth_module_name)
                # Assuming the module has an 'Authenticator' class
                auth_manager = getattr(auth_module, "Authenticator")(config)
                logger.info(
                    f"Primary authentication manager configured from {auth_module_name}."
                )
            except ImportError:
                logger.error(
                    f"Failed to import primary auth module: {auth_module_name}",
                    exc_info=True,
                )
            except AttributeError:
                logger.error(
                    f"Primary auth module {auth_module_name} does not have an 'Authenticator' attribute.",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"Failed to initialize primary authenticator from {auth_module_name}: {e}",
                    exc_info=True,
                )
        else:
            # Only log warning if login_required wasn't bypassed - otherwise it's expected
            if login_required == _login_required_impl:
                logger.warning(
                    "Primary authentication module ('module' in config) not specified. Password login may not function."
                )

    # Add Login/Logout Routes (these likely use the auth_manager)
    try:
        logger.debug("Importing login/logout route handlers.")
        # Ensure these imports happen *after* potential redefinitions above
        from . import login, logout

        logger.info("Adding /login and /logout routes.")
        application.add_route("/login", login)
        application.add_route("/logout", logout)
    except ImportError:
        logger.error(
            "Failed to import login/logout modules from current directory.",
            exc_info=True,
        )
    except Exception as e:
        logger.error(f"Failed to add login/logout routes: {e}", exc_info=True)

    logger.info("Authentication module initialization complete.")
