# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTPError  # Added for re-raising specific errors
from falcon import HTTPInternalServerError  # Added for error handling
from falcon import HTTP_200, HTTPBadRequest, HTTPNotFound

from ... import db
from ...auth import check_team_auth, login_required
from ...constants import ROSTER_USER_DELETED, ROSTER_USER_EDITED
from ...utils import create_audit, load_json_body, unsubscribe_notifications


@login_required
def on_delete(req, resp, team, roster, user):
    """
    Delete user from roster
    ... (docstring unchanged) ...
    """
    try:
        team, roster, user = unquote(team), unquote(roster), unquote(user)
    except Exception as e:
        raise HTTPBadRequest(
            "Invalid URL", f"Failed to decode URL parameters: {e}"
        ) from e

    # Auth check before DB operations
    check_team_auth(team, req)

    deleted_from_team = (
        False  # Flag to track if user was removed from team itself
    )

    try:
        with db.connect() as connection:
            cursor = connection.cursor()
            try:
                # Find the roster_id first
                cursor.execute(
                    """SELECT `id` FROM `roster`
                       WHERE `team_id` = (SELECT `id` FROM `team` WHERE name = %s)
                         AND `name` = %s""",
                    (team, roster),
                )
                roster_id_result = cursor.fetchone()
                if roster_id_result is None:
                    raise HTTPNotFound(
                        description=f"Roster '{roster}' not found for team '{team}'."
                    )
                roster_id = roster_id_result[0]  # Extract the ID

                # Delete user from the specific roster
                # Use roster_id found above
                delete_roster_user_count = cursor.execute(
                    """DELETE FROM `roster_user`
                       WHERE `roster_id`= %s
                         AND `user_id`=(SELECT `id` FROM `user` WHERE `name`=%s)""",
                    (roster_id, user),
                )
                # Optional: Check if delete_roster_user_count is 0 to indicate user wasn't in roster?

                # Delete user from associated schedule orders for this roster
                # Use roster_id found above
                cursor.execute(
                    """DELETE `schedule_order` FROM `schedule_order`
                       JOIN `schedule` ON `schedule`.`id` = `schedule_order`.`schedule_id`
                       WHERE `schedule`.`roster_id` = %s
                         AND `schedule_order`.`user_id` = (SELECT `id` FROM `user` WHERE `name` = %s)""",
                    (roster_id, user),
                )

                # Create audit log BEFORE potential final team removal
                create_audit(
                    {"roster": roster, "user": user},
                    team,
                    ROSTER_USER_DELETED,
                    req,
                    cursor,
                )

                # Check if user needs to be removed from the team entirely
                # (i.e., not in any other rosters for this team and not an admin)
                query_remove_team_user = """
                    DELETE FROM `team_user`
                    WHERE `user_id` = (SELECT `id` FROM `user` WHERE `name`=%s LIMIT 1)
                      AND `team_id` = (SELECT `id` FROM `team` WHERE `name` = %s LIMIT 1)
                      AND NOT EXISTS (
                          -- Check if user exists in ANY roster for this team
                          SELECT 1 FROM `roster_user` ru
                          JOIN `roster` r ON r.`id` = ru.`roster_id`
                          WHERE r.`team_id` = (SELECT `id` FROM `team` WHERE `name` = %s LIMIT 1)
                            AND ru.`user_id` = (SELECT `id` FROM `user` WHERE `name` = %s LIMIT 1)
                      )
                      AND NOT EXISTS (
                          -- Check if user is an admin for this team
                          SELECT 1 FROM `team_admin` ta
                          WHERE ta.`team_id` = (SELECT `id` FROM `team` WHERE `name` = %s LIMIT 1)
                            AND ta.`user_id` = (SELECT `id` FROM `user` WHERE `name` = %s LIMIT 1)
                      )"""
                remove_count = cursor.execute(
                    query_remove_team_user, (user, team, team, user, team, user)
                )

                if remove_count > 0:
                    deleted_from_team = True
                    # Unsubscribe notifications only if removed from the team itself
                    unsubscribe_notifications(team, user, cursor)

                # Commit transaction if all steps succeeded
                connection.commit()

            except HTTPNotFound as e:
                # Re-raise specific HTTP errors
                connection.rollback()
                raise e
            except Exception as e:
                # Rollback on any error during the transaction
                connection.rollback()
                print(
                    f"Error during roster user delete transaction: {e}"
                )  # Log error
                raise HTTPInternalServerError(
                    description=f"Failed to delete user from roster: {e}"
                ) from e

    except db.Error as e:
        # Handle connection errors
        print(f"Database connection error in on_delete roster user: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise other HTTP errors (like auth)
        raise e
    except Exception as e:
        # Handle unexpected errors (like param decoding)
        print(f"Unexpected error in on_delete roster user: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # Set success response outside the try/except blocks
    resp.status = HTTP_200
    resp.text = "[]"  # Or perhaps {"message": "User deleted successfully"}


@login_required
def on_put(req, resp, team, roster, user):
    """
    Put a user into/out of rotation within a given roster
    ... (docstring unchanged) ...
    """
    try:
        team, roster, user = unquote(team), unquote(roster), unquote(user)
        data = load_json_body(req)
        in_rotation_raw = data.get("in_rotation")
    except Exception as e:
        raise HTTPBadRequest(
            "Invalid Request", f"Parameter/Body processing error: {e}"
        ) from e

    if in_rotation_raw is None:
        raise HTTPBadRequest("incomplete data", 'missing field "in_rotation"')

    # Convert to integer (0 or 1) for database
    try:
        in_rotation = 1 if bool(in_rotation_raw) else 0
    except ValueError:
        raise HTTPBadRequest(
            "invalid data", 'field "in_rotation" must be boolean (true/false)'
        )

    # Auth check before DB operations
    check_team_auth(team, req)

    try:
        with db.connect() as connection:
            cursor = connection.cursor()
            try:
                # Execute the update
                rows_affected = cursor.execute(
                    """UPDATE `roster_user` SET `in_rotation`=%s
                       WHERE `user_id` = (SELECT `id` FROM `user` WHERE `name`=%s LIMIT 1)
                         AND `roster_id` =
                           (SELECT `id` FROM `roster` WHERE `name`=%s
                            AND `team_id` = (SELECT `id` FROM `team` WHERE `name` = %s LIMIT 1) LIMIT 1)""",
                    (in_rotation, user, roster, team),
                )

                # Check if the user/roster/team combination existed
                if rows_affected == 0:
                    # You might want to check if the user/roster/team exists before updating
                    # to provide a more specific 404, but for PUT, updating 0 rows
                    # can also mean the value was already set. A simple 200 OK is often acceptable.
                    # For stricter checking, uncomment below:
                    cursor.execute(
                        "SELECT 1 FROM `user` WHERE `name` = %s", (user,)
                    )
                    if not cursor.fetchone():
                        raise HTTPNotFound(
                            description=f"User '{user}' not found."
                        )
                    cursor.execute(
                        "SELECT 1 FROM `team` WHERE `name` = %s", (team,)
                    )
                    if not cursor.fetchone():
                        raise HTTPNotFound(
                            description=f"Team '{team}' not found."
                        )
                    cursor.execute(
                        "SELECT 1 FROM `roster` r JOIN `team` t ON r.team_id = t.id WHERE r.name = %s AND t.name = %s",
                        (roster, team),
                    )
                    if not cursor.fetchone():
                        raise HTTPNotFound(
                            description=f"Roster '{roster}' not found for team '{team}'."
                        )
                    # If all checks pass, then user might not be in that specific roster:
                    raise HTTPNotFound(
                        description=f"User '{user}' not found in roster '{roster}'."
                    )
                    # pass  # Or raise HTTPNotFound if strict checks needed

                # Create audit log
                create_audit(
                    {"user": user, "roster": roster, "request_body": data},
                    team,
                    ROSTER_USER_EDITED,
                    req,
                    cursor,
                )

                # Commit transaction
                connection.commit()

            except Exception as e:
                # Rollback on any error during the transaction
                connection.rollback()
                print(
                    f"Error during roster user update transaction: {e}"
                )  # Log error
                raise HTTPInternalServerError(
                    description=f"Failed to update user rotation status: {e}"
                ) from e

    except db.Error as e:
        # Handle connection errors
        print(f"Database connection error in on_put roster user: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise other HTTP errors (like auth or bad request)
        raise e
    except Exception as e:
        # Handle unexpected errors
        print(f"Unexpected error in on_put roster user: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # Set success response outside the try/except blocks
    resp.status = HTTP_200
    resp.text = "[]"  # Or {"message": "User rotation status updated"}
