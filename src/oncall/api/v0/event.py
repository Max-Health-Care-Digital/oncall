# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time

from falcon import (
    HTTP_204,
    HTTPBadRequest,
    HTTPError,
    HTTPNotFound,
    HTTPUnauthorized,
)
from ujson import dumps as json_dumps

from ... import constants, db
from ...auth import check_calendar_auth, check_team_auth, login_required
from ...constants import EVENT_DELETED, EVENT_EDITED
from ...utils import create_audit  # Assuming create_audit takes a cursor
from ...utils import (  # Assuming create_notification takes a cursor; Assuming user_in_team_by_name takes a cursor
    create_notification,
    load_json_body,
    user_in_team_by_name,
)

# Assuming all_columns_select_clause and columns are correctly defined in events.py sibling file
from .events import all_columns_select_clause, columns

# Columns which may be modified via PUT and their parameterized query snippets
update_columns = {
    "start": "`start`=%(start)s",
    "end": "`end`=%(end)s",
    "role": "`role_id`=(SELECT `id` FROM `role` WHERE `name`=%(role)s)",
    "user": "`user_id`=(SELECT `id` FROM `user` WHERE `name`=%(user)s)",
    "note": "`note`=%(note)s",
}


def on_get(req, resp, event_id):
    """
    Get event by id.

    ... (docstring remains the same) ...
    """
    # Ensure event_id is an integer
    try:
        event_id_int = int(event_id)
    except (ValueError, TypeError):
        raise HTTPBadRequest("Invalid ID", "Event ID must be an integer")

    fields = req.get_param_as_list("fields")
    select_cols = []
    if fields:
        # Validate fields and build SELECT clause
        for f in fields:
            if f not in columns:
                raise HTTPBadRequest(
                    "Bad fields", f"Invalid field requested: {f}"
                )
            select_cols.append(columns[f])
    else:
        select_cols = list(columns.values())  # Default to all columns

    cols_clause = ", ".join(select_cols)

    query = f"""SELECT {cols_clause} FROM `event`
               JOIN `user` ON `user`.`id` = `event`.`user_id`
               JOIN `team` ON `team`.`id` = `event`.`team_id`
               JOIN `role` ON `role`.`id` = `event`.`role_id`
               WHERE `event`.`id` = %s"""  # Use %s placeholder for event_id

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the query with the parameterized event_id
        cursor.execute(query, (event_id_int,))  # Pass as a tuple

        # Fetch the single result
        data = cursor.fetchone()
        # No need to check cursor.rowcount != 0, fetchone returning None is sufficient
        # num_found = cursor.rowcount # This is often unreliable after fetchone

        # Check if data was found within the with block
        if not data:
            raise HTTPNotFound(
                description=f"Event with ID {event_id} not found"
            )

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block
    resp.text = json_dumps(data)


@login_required
def on_put(req, resp, event_id):
    """
    Update an event by id; anyone can update any event within the team

    ... (docstring remains the same) ...
    """
    # Ensure event_id is an integer
    try:
        event_id_int = int(event_id)
    except (ValueError, TypeError):
        raise HTTPBadRequest("Invalid ID", "Event ID must be an integer")

    data = load_json_body(req)  # Dictionary of fields to update

    # Perform initial validation checks on incoming data before DB interaction
    if (
        "end" in data
        and "start" in data
        and data["start"] is not None
        and data["end"] is not None
        and data["start"] >= data["end"]
    ):
        raise HTTPBadRequest(
            "Invalid event update", "Event must start before it ends"
        )

    # Check for invalid columns before building the update query
    invalid_cols = [col for col in data.keys() if col not in update_columns]
    if invalid_cols:
        raise HTTPBadRequest(
            "Invalid event update",
            f"Invalid column(s) provided: {', '.join(invalid_cols)}",
        )

    # Build the SET clause and the data dictionary for parameters
    set_clause_snippets = []
    update_data_params = {}  # Use a new dict for parameters

    # Always nullify link_id on update as per original logic
    set_clause_snippets.append("`link_id` = NULL")

    for col, value in data.items():
        # Only include columns that are valid for update and are in the request body
        if col in update_columns:
            set_clause_snippets.append(
                update_columns[col]
            )  # Add the snippet with %(name)s placeholder
            update_data_params[col] = (
                value  # Add the value to the parameters dict
            )

    # If there are no columns to update (except implicit link_id=NULL), return 204
    # Check if any explicit update columns were requested
    if (
        not set_clause_snippets
    ):  # Should not happen with link_id = NULL always added, but defensive
        resp.status = HTTP_204  # No content to update
        return

    set_clause = ", ".join(set_clause_snippets)

    # Construct the UPDATE query template using parameterized values
    # *** FIX: Use %s placeholder for event_id in WHERE clause ***
    update_query = f"UPDATE `event` SET {set_clause} WHERE `id`=%(event_id)s"
    # Add event_id to the parameters dictionary for the WHERE clause
    update_data_params["event_id"] = event_id_int

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching event data

        # 1. Fetch existing event data for validation, audit, and notification
        cursor.execute(
            """SELECT
                `event`.`start`,
                `event`.`end`,
                `event`.`user_id`,
                `event`.`role_id`,
                `event`.`id`,
                `event`.`note`,
                `team`.`name` AS `team`,
                `role`.`name` AS `role`,
                `user`.`name` AS `user`,
                `user`.`full_name`,
                `event`.`team_id`
            FROM `event`
            JOIN `team` ON `event`.`team_id` = `team`.`id`
            JOIN `role` ON `event`.`role_id` = `role`.`id`
            JOIN `user` ON `event`.`user_id` = `user`.`id`
            WHERE `event`.`id`=%s""",
            (event_id_int,),  # Parameterize event_id as a tuple
        )
        event_data = cursor.fetchone()  # Fetch the single result

        # Check if event was found within the with block
        if not event_data:
            raise HTTPNotFound(
                description=f"Event with ID {event_id} not found for update"
            )

        # 2. Perform authorization checks
        check_calendar_auth(
            event_data["team"], req
        )  # Check general calendar auth

        # 3. Perform timestamp validation and admin override check
        now = time.time()
        # Determine the new start and end times based on provided data or original event_data
        new_start = data.get("start", event_data["start"])
        new_end = data.get("end", event_data["end"])

        # Check if editing a past event (start time is in the past)
        is_past_event = event_data["start"] < now - constants.GRACE_PERIOD
        is_new_start_in_past = new_start < now - constants.GRACE_PERIOD

        if is_past_event or is_new_start_in_past:
            # Check if *only* the end time is being extended into the future
            is_only_extending_end_into_future = (
                event_data["start"] == new_start  # Start time is unchanged
                and event_data["role_id"]
                == data.get(
                    "role_id", event_data["role_id"]
                )  # Role is unchanged (or same)
                and event_data["user_id"]
                == data.get(
                    "user_id", event_data["user_id"]
                )  # User is unchanged (or same)
                and new_end > now  # New end time is in the future
            )

            # If it's a past event edit AND not just extending the end time into the future
            if not is_only_extending_end_into_future:
                # This edit requires admin privileges for the team
                try:
                    check_team_auth(
                        event_data["team"], req
                    )  # Check admin auth for the team
                except HTTPUnauthorized:
                    # If unauthorized, it's a bad request because the edit is not allowed without admin
                    raise HTTPBadRequest(
                        "Invalid event update",
                        "Editing past events (or setting start time in past) not allowed without team admin privileges",
                    )

        # 4. Check if the new user (if updated) is part of the team
        if "user" in data:  # Only check if user is being updated
            new_user_name = data["user"]
            # Assuming user_in_team_by_name takes a cursor and handles DB ops within it
            if not user_in_team_by_name(
                cursor, new_user_name, event_data["team"]
            ):
                # Raise exception within the with block
                raise HTTPBadRequest(
                    "Invalid event update",
                    f"New event user '{new_user_name}' must be part of team '{event_data['team']}'",
                )

        # 5. Execute the UPDATE query
        try:
            # Execute the UPDATE query using the prepared template and parameters dictionary
            cursor.execute(update_query, update_data_params)

            # Optional: Check if the update affected exactly one row
            # if cursor.rowcount != 1:
            #      # This could happen if the ID wasn't found (already checked),
            #      # or somehow affected multiple rows (indicates data problem).
            #      # Raising a server error as it's unexpected.
            #      raise HTTPError("500 Internal Server Error", "Database Error", f"Unexpected number of rows updated for event ID {event_id_int}")

            # 6. Create audit log
            # Prepare new_event context for audit (using provided data for changes)
            audit_new_event_context = ", ".join(
                f"{key}: {value}" for key, value in data.items()
            )
            create_audit(
                {
                    "old_event": event_data,
                    "request_body": data,
                    "new_event_context": audit_new_event_context,
                },
                event_data["team"],  # Team name for audit
                EVENT_EDITED,
                req,
                cursor,  # Pass the cursor
            )

            # 7. Create notification
            # Select new event data after update for notification (esp. if role/user changed)
            cursor.execute(
                "SELECT `user_id`, role_id, `start` FROM `event` WHERE `id` = %s",  # Also fetch start for notification
                (event_id_int,),  # Parameterize event_id as a tuple
            )
            new_ev_data = cursor.fetchone()
            if (
                not new_ev_data
            ):  # Should not happen if update and previous select worked
                raise HTTPError(
                    "500 Internal Server Error",
                    "Database Error",
                    f"Could not retrieve new info for event ID {event_id_int} after update",
                )

            # Prepare context for notification
            notification_context = {
                "full_name": event_data[
                    "full_name"
                ],  # Original full name? Or fetch new one? Original code used old.
                "role": event_data[
                    "role"
                ],  # Original role name? Or fetch new one? Original code used old.
                "team": event_data["team"],  # Original team name
                "new_event_details": audit_new_event_context,  # Provide details of the change
            }

            # Create notification using the same cursor
            # Notification needs original and new user/role IDs and event start time
            original_user_id = event_data["user_id"]
            original_role_id = event_data["role_id"]
            new_user_id = new_ev_data["user_id"]
            new_role_id = new_ev_data["role_id"]
            original_start_time = event_data[
                "start"
            ]  # Use original start time for notification context? Original code did.

            create_notification(
                notification_context,
                event_data["team_id"],  # Team ID
                {original_role_id, new_role_id},  # Roles affected
                EVENT_EDITED,
                {original_user_id, new_user_id},  # Users affected
                cursor,  # Pass the cursor
                start_time=original_start_time,  # Use original start time for notification context?
            )

            # 8. Commit the transaction if all steps in the try block succeed
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            # Check for potential IntegrityError messages from subqueries (non-existent user, role, team)
            if "Column 'role_id' cannot be null" in err_msg:
                # This could happen if the provided 'role' name in data['role'] doesn't exist
                err_msg = f'New role "{data.get("role")}" not found'
            elif "Column 'user_id' cannot be null" in err_msg:
                # This could happen if the provided 'user' name in data['user'] doesn't exist
                err_msg = f'New user "{data.get("user")}" not found'
            # Add other potential IntegrityError checks if applicable (e.g., foreign key to team_id)
            else:
                # Generic fallback for other integrity errors
                err_msg = f"Database Integrity Error: {err_msg}"

            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e

        # Any other exception raised in the try block will also trigger rollback and cleanup.
        # Do not need finally block or bare except; rely on the 'with' statement.

    resp.status = HTTP_204  # Standard response for successful PUT with no body


@login_required
def on_delete(req, resp, event_id):
    """
    Delete an event by id, anyone on the team can delete that team's events

    ... (docstring remains the same) ...
    """
    # Ensure event_id is an integer
    try:
        event_id_int = int(event_id)
    except (ValueError, TypeError):
        raise HTTPBadRequest("Invalid ID", "Event ID must be an integer")

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching event data

        # 1. Fetch event data for auth, audit, and notification
        # This also checks if the event exists.
        cursor.execute(
            """SELECT `team`.`name` AS `team`, `event`.`team_id`, `role`.`name` AS `role`,
                                 `event`.`role_id`, `event`.`start`, `user`.`full_name`, `event`.`user_id`
                          FROM `event`
                          JOIN `team` ON `event`.`team_id` = `team`.`id`
                          JOIN `role` ON `event`.`role_id` = `role`.`id`
                          JOIN `user` ON `event`.`user_id` = `user`.`id`
                          WHERE `event`.`id` = %s""",
            (event_id_int,),  # Parameterize event_id
        )
        ev_info = cursor.fetchone()  # Fetch the single result

        # Check if event was found within the with block
        if not ev_info:
            # Event not found, raise 404 immediately within the with block
            raise HTTPNotFound(
                description=f"Event with ID {event_id} not found for deletion"
            )

        try:
            # 2. Perform authorization checks
            check_calendar_auth(
                ev_info["team"], req
            )  # Check general calendar auth

            # 3. Perform past event validation
            now = time.time()
            if ev_info["start"] < now - constants.GRACE_PERIOD:
                # Editing past events requires admin, but deleting past events seems disallowed entirely based on message
                raise HTTPBadRequest(
                    "Invalid event deletion",
                    "Deleting events in the past not allowed",
                )

            # 4. Execute the DELETE query
            cursor.execute(
                "DELETE FROM `event` WHERE `id`=%s", (event_id_int,)
            )  # Parameterize event_id

            # Optional: Check rowcount == 1 if needed, though NotFound is handled above.
            # if cursor.rowcount != 1:
            #      raise HTTPError("500 Internal Server Error", "Database Error", f"Unexpected number of rows deleted for event ID {event_id_int}")

            # 5. Create notification
            context = {
                "team": ev_info["team"],
                "full_name": ev_info["full_name"],
                "role": ev_info["role"],
            }
            # Assuming create_notification takes a cursor and handles DB ops within it
            create_notification(
                context,
                ev_info["team_id"],
                [ev_info["role_id"]],
                EVENT_DELETED,
                [ev_info["user_id"]],
                cursor,  # Pass the cursor
                start_time=ev_info["start"],  # Use original start time
            )

            # 6. Create audit trail entry
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit(
                {"old_event": ev_info},
                ev_info["team"],
                EVENT_DELETED,
                req,
                cursor,
            )  # Use ev["team"] as original code

            # 7. Commit the transaction if all steps in the try block succeed
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during event delete transaction for event ID {event_id_int}: {e}"
            )  # Replace with logging
            # Re-raise the exception (unless it was an HTTP exception we raised ourselves, Falcon handles those)
            # Falcon typically handles HTTP exceptions raised within the endpoint.
            # Re-raising other exceptions lets them propagate for Falcon to handle (e.g., 500 Internal Server Error).
            # The original code had a bare except and re-raised, which hides type. Better to let Falcon catch.
            # However, the original code used a finally. With 'with', simply let it propagate.
            # If a specific non-HTTP exception needs custom handling/logging, add specific except blocks.
            # For now, just let unexpected errors propagate.
            raise

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_204  # Standard response for successful DELETE
