# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTPInternalServerError  # Added for error handling
from falcon import HTTP_201, HTTPBadRequest, HTTPError, HTTPNotFound
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required
from ...constants import ROSTER_USER_ADDED
from ...utils import create_audit, load_json_body, subscribe_notifications
from .users import get_user_data


def on_get(req, resp, team, roster):
    """
    Get all users for a team's roster

    **Example request**: ...
    **Example response**: ...
    """
    try:
        team, roster = unquote(team), unquote(roster)
        in_rotation = req.get_param_as_bool("in_rotation")
    except Exception as e:
        # Handle potential errors during unquote or param processing
        raise HTTPBadRequest(
            "Invalid Request", f"Parameter processing error: {e}"
        ) from e

    data = []  # Initialize data outside try block
    try:
        with db.connect() as connection:
            # Using a standard cursor as we only need one column
            cursor = connection.cursor()
            query = """SELECT `user`.`name` FROM `user`
                       JOIN `roster_user` ON `roster_user`.`user_id`=`user`.`id`
                       JOIN `roster` ON `roster`.`id`=`roster_user`.`roster_id`
                       JOIN `team` ON `team`.`id`=`roster`.`team_id`
                       WHERE `roster`.`name`=%s AND `team`.`name`=%s"""
            query_params = [roster, team]

            if in_rotation is not None:
                query += " AND `roster_user`.`in_rotation` = %s"
                query_params.append(in_rotation)

            cursor.execute(query, tuple(query_params))  # Pass params as tuple
            data = [row[0] for row in cursor.fetchall()]
            # No cursor.close() or connection.close() needed

    except db.Error as e:
        print(f"Database error in on_get roster users: {e}")
        raise HTTPInternalServerError(
            description="Failed to retrieve roster users due to database error."
        )
    except Exception as e:
        print(f"Unexpected error in on_get roster users: {e}")
        raise HTTPInternalServerError(
            description="An unexpected error occurred while retrieving roster users."
        )

    resp.text = json_dumps(data)


@login_required
def on_post(req, resp, team, roster):
    """
    Add user to a roster for a team. On successful creation, returns that user's information.
    ... (docstring unchanged) ...
    """
    try:
        team, roster = unquote(team), unquote(roster)
        data = load_json_body(req)
        user_name = data.get("name")
        # Ensure in_rotation is treated as boolean 0 or 1 for DB consistency
        in_rotation = 1 if data.get("in_rotation", True) else 0
    except Exception as e:
        raise HTTPBadRequest(
            "Invalid Request", f"Parameter/Body processing error: {e}"
        ) from e

    if not user_name:
        raise HTTPBadRequest("incomplete data", 'missing field "name"')

    # Perform auth check before database operations
    check_team_auth(team, req)

    user_data_response = None  # Initialize response data

    try:
        with db.connect() as connection:
            cursor = connection.cursor()
            try:
                # Check team and user existence first
                cursor.execute(
                    """(SELECT `id` FROM `team` WHERE `name`=%s)
                       UNION ALL
                       (SELECT `id` FROM `user` WHERE `name`=%s)""",
                    (team, user_name),
                )
                results = [r[0] for r in cursor.fetchall()]
                if len(results) < 2:
                    # Check which one is missing for a more specific error? Optional.
                    # Example check: cursor.execute("SELECT 1 FROM team WHERE name=%s", (team,)) etc.
                    raise HTTPError(
                        "422 Unprocessable Entity",
                        "IntegrityError",
                        "Invalid team or user specified.",
                    )
                team_id, user_id = results

                # Add user to team members if not already there (IGNORE handles existing)
                # IMPORTANT: Changed %r to %s for safe parameterization
                cursor.execute(
                    """INSERT IGNORE INTO `team_user` (`team_id`, `user_id`) VALUES (%s, %s)""",
                    (team_id, user_id),
                )

                # Get roster ID and calculate next roster priority
                cursor.execute(
                    """SELECT `roster`.`id`, COALESCE(MAX(`roster_user`.`roster_priority`), -1) + 1
                       FROM `roster`
                       LEFT JOIN `roster_user` ON `roster`.`id` = `roster_user`.`roster_id`
                       JOIN `team` ON `team`.`id`=`roster`.`team_id`
                       WHERE `team`.`name`=%s AND `roster`.`name`=%s
                       GROUP BY `roster`.`id`""",  # Added GROUP BY for safety with MAX aggregate
                    (team, roster),
                )
                roster_result = cursor.fetchone()
                if not roster_result:
                    # Roster or team not found (should have been caught earlier by team check if team missing)
                    raise HTTPNotFound(
                        description=f"Roster '{roster}' not found for team '{team}'."
                    )
                roster_id, roster_priority = roster_result

                # Insert the user into the specific roster
                cursor.execute(
                    """INSERT INTO `roster_user` (`user_id`, `roster_id`, `in_rotation`, `roster_priority`)
                       VALUES (%s, %s, %s, %s)""",
                    (user_id, roster_id, in_rotation, roster_priority),
                )

                # Add user to associated schedule orders
                cursor.execute(
                    """INSERT INTO `schedule_order` (`schedule_id`, `user_id`, `priority`)
                       SELECT
                           `schedule`.`id`,
                           %s,
                           COALESCE(MAX(`so`.`priority`), -1) + 1
                       FROM `schedule`
                       JOIN `roster` ON `roster`.`id` = `schedule`.`roster_id`
                       LEFT JOIN `schedule_order` so ON `schedule`.`id` = `so`.`schedule_id`
                       WHERE `roster`.`id` = %s
                       GROUP BY `schedule`.`id`""",
                    (user_id, roster_id),  # Use roster_id found earlier
                )

                # Subscribe user to notifications (passing cursor for potential DB ops)
                subscribe_notifications(team, user_name, cursor)

                # Create audit log entry (passing cursor for potential DB ops)
                create_audit(
                    {"roster": roster, "user": user_name, "request_body": data},
                    team,
                    ROSTER_USER_ADDED,
                    req,
                    cursor,
                )

                # Fetch user data for the response *before* commit (read operation)
                # Assuming get_user_data uses its own connection or can use the existing cursor safely
                # If get_user_data modifies data or needs a fresh connection, call it outside 'with'
                # after ensuring commit succeeded, or refactor it.
                # Let's assume it can run here before commit for now.
                user_data_list = get_user_data(
                    None, {"name": user_name}
                )  # Pass cursor if needed
                if not user_data_list:
                    # Should not happen if user existed, but good practice
                    raise HTTPInternalServerError(
                        description="Failed to fetch data for newly added user."
                    )
                user_data_response = user_data_list[0]

                # Commit the transaction if all operations were successful
                connection.commit()

            except db.IntegrityError as e:
                # Specific error for duplicate entry
                connection.rollback()  # Rollback on integrity error
                # Check if the error is specifically about duplicate entry if possible
                # e.g. if 'Duplicate entry' in str(e):
                raise HTTPError(
                    "422 Unprocessable Entity",
                    "IntegrityError",
                    f'User "{user_name}" is already in the roster "{roster}" for team "{team}".',
                ) from e
            except HTTPNotFound as e:
                # Re-raise HTTPNotFound if roster wasn't found
                connection.rollback()
                raise e
            except Exception as e:
                # Catch any other database or unexpected error during the transaction
                connection.rollback()  # Rollback on any other error within the try
                print(
                    f"Error during roster user add transaction: {e}"
                )  # Log error
                raise HTTPInternalServerError(
                    description=f"Failed to add user to roster: {e}"
                ) from e

            # If we reach here, the transaction was committed successfully

    except db.Error as e:
        # Error connecting to the database
        print(f"Database connection error in on_post roster user: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise HTTPError exceptions directly (like 422 from checks)
        raise e
    except Exception as e:
        # Catch other unexpected errors (e.g., during param processing)
        print(f"Unexpected error in on_post roster user: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # Set success response outside the main try/except if commit was successful
    if user_data_response:
        resp.status = HTTP_201
        resp.text = json_dumps(user_data_response)
    else:
        # This case should ideally not be reached if logic is correct,
        # but as a fallback:
        raise HTTPInternalServerError(
            description="Failed to get user data after adding to roster."
        )
