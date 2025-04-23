# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import logging
from typing import Any, Dict # Added for type hints

from falcon import (HTTP_201, HTTPBadRequest, HTTPError, Request, Response, # Added Request, Response
                    HTTPNotFound) # Added HTTPNotFound for potential future use

# Assuming db module provides connect() returning the safe wrapper, Error, IntegrityError, DictCursor
from ... import db
from ...auth import check_team_auth, login_required
from ...utils import load_json_body

# Use a specific logger for this module if desired
logger = logging.getLogger(__name__)


def on_get(req: Request, resp: Response, team: str) -> None:
    """
    Gets all subscriptions for a given team (Safe Version using ContextualRawConnection).
    """
    # NOTE: Using %s placeholder style assuming the DBAPI driver supports it.
    # Replace with ? or :name if your driver uses a different style for parameterized queries.
    sql = """
        SELECT `subscription`.`name` AS `subscription`, `role`.`name` AS `role`
        FROM `team`
        JOIN `team_subscription` ON `team`.`id` = `team_subscription`.`team_id`
        JOIN `team` `subscription` ON `subscription`.`id` = `team_subscription`.`subscription_id`
        JOIN `role` ON `role`.`id` = `team_subscription`.`role_id`
        WHERE `team`.`name` = %s
    """
    data = []

    try:
        # Use context manager for connection (returns wrapper)
        with db.connect() as connection_wrapper:
            cursor = None
            try:
                # Request DictCursor if available and needed for dictionary access below
                dict_cursor_cls = getattr(db, 'DictCursor', None)
                if not dict_cursor_cls:
                    logger.warning('db.DictCursor not available for this driver. Row access might need adjustment.')
                cursor_args = (dict_cursor_cls,) if dict_cursor_cls else ()
                cursor = connection_wrapper.cursor(*cursor_args)

                # Use parameterized query for safety
                cursor.execute(sql, (team,))

                # fetchall() should return a list of rows (dicts if DictCursor worked)
                data = cursor.fetchall()

            finally:
                # Ensure cursor is closed manually since cursor context manager isn't guaranteed
                if cursor:
                    try:
                        cursor.close()
                    except Exception as cur_e:
                        logger.warning(f'Error closing cursor: {cur_e}', exc_info=True)

        # Falcon automatically handles JSON conversion for resp.media
        resp.media = data

    except db.Error as e:
        logger.error(f"Database error fetching subscriptions for team '{team}': {e}", exc_info=True)
        raise HTTPError('500 Internal Server Error', description=f'Database Error: {e}')
    except Exception as e:
        logger.error(f"Unexpected error fetching subscriptions for team '{team}': {e}", exc_info=True)
        raise HTTPError('500 Internal Server Error', description=f'Unexpected Error: {e}')


@login_required
def on_post(req: Request, resp: Response, team: str) -> None:
    """
    Adds a subscription for a team (Safe Version using ContextualRawConnection).

    Subscribes 'team' to notifications for 'role' on 'subscription' team's schedule.
    """
    # Ensure auth check doesn't need DB or handles it safely
    check_team_auth(team, req)
    data = load_json_body(req)

    sub_name = data.get("subscription")
    role_name = data.get("role")

    # Validate input parameters
    if not sub_name or not role_name:
        raise HTTPBadRequest("Missing parameter(s)", description="Required 'subscription' (team name) and 'role' parameters missing from JSON body")

    if sub_name == team:
        raise HTTPBadRequest("Invalid subscription", description="Subscription team must be different from the subscribing team")

    # NOTE: Using %s placeholder style
    sql = """
        INSERT INTO `team_subscription` (`team_id`, `subscription_id`, `role_id`)
        VALUES (
            (SELECT `id` FROM `team` WHERE `name` = %s),
            (SELECT `id` FROM `team` WHERE `name` = %s),
            (SELECT `id` FROM `role` WHERE `name` = %s)
        )
    """

    try:
        # Use context manager for connection (returns wrapper)
        with db.connect() as connection_wrapper:
            cursor = None
            try:
                # Don't necessarily need DictCursor for INSERT
                cursor = connection_wrapper.cursor()

                # Execute the insert using parameters
                cursor.execute(sql, (team, sub_name, role_name))

                # Commit the transaction using the wrapper *after* successful execution
                connection_wrapper.commit()
                logger.info(f"Successfully added subscription for team '{team}' to team '{sub_name}' role '{role_name}'.")
                resp.status = HTTP_201 # Set status on success

            except db.IntegrityError as e:
                # Rollback transaction on integrity error
                try:
                    logger.warning(f"Rolling back transaction due to IntegrityError: {e}")
                    connection_wrapper.rollback()
                except Exception as rb_e:
                    # Log rollback failure but proceed to raise original error context
                    logger.error(f'Rollback failed after IntegrityError: {rb_e}', exc_info=True)

                logger.warning(f'IntegrityError adding subscription for team "{team}" to team "{sub_name}" role "{role_name}": {e}')
                # Try to determine cause without fragile string parsing, using codes if available
                error_code = e.args[0] if isinstance(e.args, tuple) and len(e.args) > 0 else None
                if error_code == 1062: # MySQL duplicate entry code
                    err_msg = f'Subscription for team "{team}" to team "{sub_name}" role "{role_name}" already exists.'
                    # Use 409 Conflict or 400 Bad Request for duplicates
                    raise HTTPBadRequest("Subscription exists", description=err_msg) from e
                elif error_code == 1048: # MySQL column cannot be null (often due to non-existent FK)
                     err_msg = f'Team "{team}", Subscribing Team "{sub_name}", or Role "{role_name}" not found (or other required value missing).'
                     raise HTTPError("422 Unprocessable Entity", "Not Found or Invalid Reference", description=err_msg) from e
                else: # Generic integrity error
                    err_msg = f"Database integrity constraint violated when adding subscription. Please check if teams and roles exist."
                    logger.debug(f"Original IntegrityError details: {e}") # Log details for debugging
                    raise HTTPError("422 Unprocessable Entity", "Integrity Constraint Failed", description=err_msg) from e

            except db.Error as db_err: # Catch other DB errors during execute
                 try:
                     # Rollback if commit wasn't reached
                     connection_wrapper.rollback()
                 except Exception as rb_e:
                    logger.warning(f'Rollback failed after db.Error: {rb_e}', exc_info=True)
                 logger.error(f"Database error adding subscription: {db_err}", exc_info=True)
                 raise HTTPError('500 Internal Server Error', description=f'Database Execution Error: {db_err}') from db_err
            finally:
                # Ensure cursor is closed manually
                if cursor:
                    try:
                        cursor.close()
                    except Exception as cur_e:
                        logger.warning(f'Error closing cursor: {cur_e}', exc_info=True)

    except HTTPError as e:
        # Re-raise HTTP errors raised explicitly (e.g., 400, 422 from DB block)
        raise e
    except Exception as e:
        # Catch unexpected errors outside the DB block (e.g., load_json_body, check_team_auth)
        logger.error(f"Unexpected error adding subscription for team '{team}' to team '{sub_name}': {e}", exc_info=True)
        raise HTTPError('500 Internal Server Error', description=f'Unexpected Server Error: {e}')