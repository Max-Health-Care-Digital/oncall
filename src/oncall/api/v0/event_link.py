# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time
from operator import itemgetter

from falcon import HTTP_204, HTTPBadRequest, HTTPError, HTTPNotFound
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

# Assuming update_columns is correctly defined and imported from events.py sibling file
# It maps field names to SQL snippets using dictionary placeholders (%(name)s) or direct updates
update_columns = {
    "start": "`start`=%(start)s",  # Direct update, value is %(start)s
    "end": "`end`=%(end)s",  # Direct update, value is %(end)s
    "role": "`role_id`=(SELECT `id` FROM `role` WHERE `name`=%(role)s)",  # Subquery, value is %(role)s
    "user": "`user_id`=(SELECT `id` FROM `user` WHERE `name`=%(user)s)",  # Subquery, value is %(user)s
    "note": "`note`=%(note)s",  # Direct update, value is %(note)s
}


@login_required
def on_delete(req, resp, link_id):
    """
    Delete a set of linked events using the link_id, anyone on the team can delete that team's events

    **Example request:**

    .. sourcecode:: http

       DELETE /api/v0/events/link/1234 HTTP/1.1

    :statuscode 200: Successful delete
    :statuscode 403: Delete not allowed; logged in user is not a team member
    :statuscode 404: Events not found
    """
    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # 1. Fetch linked event data for auth, audit, and notification
        # This also checks if events with this link_id exist.
        cursor.execute(
            """SELECT `team`.`name` AS `team`, `event`.`team_id`, `role`.`name` AS `role`,
                             `event`.`role_id`, `event`.`start`, `user`.`full_name`, `event`.`user_id`
                      FROM `event`
                      JOIN `team` ON `event`.`team_id` = `team`.`id`
                      JOIN `role` ON `event`.`role_id` = `role`.`id`
                      JOIN `user` ON `event`.`user_id` = `user`.`id`
                      WHERE `event`.`link_id` = %s
                      ORDER BY `event`.`start`""",
            (link_id,),  # Parameterize link_id
        )
        data = cursor.fetchall()  # Fetch all results

        # Check if events were found within the with block
        if not data:
            # No events found with this link_id, raise 404 immediately within the with block
            raise HTTPNotFound(
                description=f"No events found with link ID {link_id} for deletion"
            )

        # Get info from the first event (original code used data[0])
        ev_info = data[0]
        # Get the minimum start time across all linked events for past event check
        event_start = min(data, key=itemgetter("start"))["start"]

        try:
            # 2. Perform authorization checks
            check_calendar_auth(
                ev_info["team"], req
            )  # Check general calendar auth for the team

            # 3. Perform past event validation
            now = time.time()
            if event_start < now - constants.GRACE_PERIOD:
                # Deleting events starting in the past is not allowed
                raise HTTPBadRequest(
                    "Invalid event deletion",
                    "Deleting events starting in the past not allowed",
                )

            # 4. Execute the DELETE query for all linked events
            cursor.execute(
                "DELETE FROM `event` WHERE `link_id`=%s", (link_id,)
            )  # Parameterize link_id

            # Optional: Check rowcount > 0 if needed, though NotFound is handled above.
            # if cursor.rowcount == 0: # Should not happen if data was non-empty
            #      raise HTTPError("500 Internal Server Error", "Database Error", f"Unexpected number of rows deleted for link ID {link_id}")

            # 5. Create notification
            context = {
                "team": ev_info["team"],
                "full_name": ev_info[
                    "full_name"
                ],  # Using full name from the first event fetched
                "role": ev_info[
                    "role"
                ],  # Using role name from the first event fetched
                "link_id": link_id,  # Add link_id to context for notification
            }
            # Use info from the first event for notification (original code used ev which was data[0])
            create_notification(
                context,
                ev_info["team_id"],
                [ev_info["role_id"]],  # Role ID from the first event
                EVENT_DELETED,
                [ev_info["user_id"]],  # User ID from the first event
                cursor,  # Pass the cursor
                start_time=event_start,  # Use minimum start time for notification context?
            )

            # 6. Create audit trail entry
            # Log details of all deleted events
            create_audit(
                {"deleted_events_data": data, "link_id": link_id},
                ev_info["team"],  # Team name from the first event
                EVENT_DELETED,
                req,
                cursor,  # Pass the cursor
            )

            # 7. Commit the transaction if all steps in the try block succeed
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during linked event delete transaction for link ID {link_id}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_204  # Standard response for successful DELETE


@login_required
def on_put(req, resp, link_id):
    """
    Update an event by link_id; anyone can update any event within the team.
    Only username can be updated using this endpoint.

    **Example request:**

    .. sourcecode:: http

        PUT /api/v0/events/link/1234 HTTP/1.1
        Content-Type: application/json

        {
            "user": "asmith",
        }

    :statuscode 200: Successful update
    """
    data = load_json_body(req)  # Dictionary of fields to update

    # Basic validation checks on incoming data before DB interaction
    # Check for invalid columns in the request body against allowed update_columns keys
    invalid_cols = [col for col in data.keys() if col not in update_columns]
    if invalid_cols:
        raise HTTPBadRequest(
            "Invalid event update",
            f"Invalid column(s) provided: {', '.join(invalid_cols)}",
        )

    # Ensure at least one valid column is provided for update
    if not data:
        # Original code would likely proceed and update 0 rows, then maybe 404.
        # Returning 204 is clearer if no update data is sent.
        resp.status = HTTP_204
        return

    # Use the 'with' statement for safe connection and transaction management
    # The entire PUT operation should be one transaction
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching event data

        # 1. Fetch existing linked event data for validation, audit, and notification
        cursor.execute(
            """SELECT
                        `event`.`start`,
                        `event`.`end`,
                        `event`.`user_id`,
                        `event`.`role_id`,
                        `event`.`id`,
                        `team`.`name` AS `team`,
                        `role`.`name` AS `role`,
                        `user`.`name` AS `user`,
                        `user`.`full_name`,
                        `event`.`team_id`,
                        `event`.`note` # Include note in select for audit/notification context
                    FROM `event`
                    JOIN `team` ON `event`.`team_id` = `team`.`id`
                    JOIN `role` ON `event`.`role_id` = `role`.`id`
                    JOIN `user` ON `event`.`user_id` = `user`.`id`
                    WHERE `event`.`link_id`=%s""",
            (link_id,),  # Parameterize link_id
        )
        event_data_list = cursor.fetchall()  # Fetch all linked events

        # Check if events were found with this link_id
        if not event_data_list:
            raise HTTPNotFound(
                description=f"No events found with link ID {link_id} for update"
            )

        # Get summary info from linked events (original logic)
        event_summary = event_data_list[
            0
        ].copy()  # Copy data from the first event
        if len(event_data_list) > 1:
            event_summary["end"] = max(event_data_list, key=itemgetter("end"))[
                "end"
            ]
            event_summary["start"] = min(
                event_data_list, key=itemgetter("start")
            )["start"]
        # If only one event, start/end are already correct from the first event.

        # 2. Perform authorization checks
        check_team_auth(
            event_summary["team"], req
        )  # Check team admin auth (original code used this)
        # check_calendar_auth(event_summary["team"], req) # Original code also called this here - keeping for compatibility if needed

        # 3. Perform timestamp validation and admin override check
        now = time.time()
        # Use the minimum start time of the linked events for the past check
        linked_events_min_start = event_summary["start"]

        if linked_events_min_start < now - constants.GRACE_PERIOD:
            # Editing past events requires admin, but link PUT logic might be simpler.
            # Original code raised HTTPBadRequest directly, implying no admin override for linked events PUT.
            raise HTTPBadRequest(
                "Invalid event update",
                "Editing events starting in the past not allowed via link ID update",
            )

        # 4. Check if the new user (if updated) is part of the team
        # Get the target user name - either from data['user'] or the original user of the first event
        target_user_name = data.get("user", event_summary["user"])

        # Assuming user_in_team_by_name takes a cursor and handles DB ops within it
        if not user_in_team_by_name(
            cursor, target_user_name, event_summary["team"]
        ):
            # Raise exception within the with block
            raise HTTPBadRequest(
                "Invalid event update",
                f"New event user '{target_user_name}' must be part of team '{event_summary['team']}'",
            )

        # 5. Construct the UPDATE query using parameterized values
        # Collect update snippets and corresponding values for dictionary parameters
        set_clause_snippets = []
        update_data_params = {}  # Dictionary for parameters

        # Add snippets and values for each valid update column in the request body
        for col, value in data.items():
            if col in update_columns:
                set_clause_snippets.append(
                    update_columns[col]
                )  # Add the snippet with %(name)s placeholder
                update_data_params[col] = (
                    value  # Add the value to the parameters dict
                )
            # else: invalid columns already checked

        # If there are no columns to update, return 204 (should be caught by initial data check)
        if not set_clause_snippets:  # Defensive check
            resp.status = HTTP_204
            return

        set_clause = ", ".join(set_clause_snippets)

        # Update link_id to NULL implicitly on update, as per original logic in single event PUT
        # Add this to the SET clause snippets if not already present
        if "`link_id` = NULL" not in set_clause_snippets:
            set_clause_snippets.append("`link_id` = NULL")
            set_clause = ", ".join(set_clause_snippets)  # Rejoin

        # Construct the final UPDATE query template
        # Use dictionary-style placeholder for link_id in WHERE clause
        update_query = (
            f"UPDATE `event` SET {set_clause} WHERE `link_id`=%(link_id)s"
        )
        # Add link_id to the parameters dictionary for the WHERE clause
        update_data_params["link_id"] = link_id  # Use the original link_id

        # 6. Execute the UPDATE query
        try:
            # Execute the UPDATE query using the prepared template and parameters dictionary
            # The DBAPI will map %(key)s placeholders to the keys in the update_data_params dictionary.
            cursor.execute(update_query, update_data_params)

            # Optional: Check if the update affected any rows (> 0)
            # If len(event_data_list) > 0 (checked already), we expect rowcount > 0.
            # If rowcount == 0, it's unexpected if link_id exists.
            # The original code raised HTTPNotFound here, which is misleading after finding events.
            # Let's raise a server error if 0 rows were updated unexpectedly.
            if cursor.rowcount == 0:
                raise HTTPError(
                    "500 Internal Server Error",
                    "Database Error",
                    f"Unexpectedly updated 0 rows for link ID {link_id}",
                )

            # 7. Create audit log
            # Prepare context for audit (using provided data for changes)
            audit_update_context = ", ".join(
                f"{key}: {data.get(key)}"
                for key in data.keys()
                if key in update_columns
            )  # Only include valid updated fields
            create_audit(
                {
                    "old_events_data": event_data_list,
                    "request_body": data,
                    "update_context": audit_update_context,
                },
                event_summary["team"],  # Team name from the first event
                EVENT_EDITED,
                req,
                cursor,  # Pass the cursor
            )

            # 8. Create notification
            # Select new event data after update for notification (esp. user/role changes across all linked events)
            # Need user_id and role_id for notification
            cursor.execute(
                "SELECT `user_id`, `role_id`, `start` FROM `event` WHERE `link_id` = %s",  # Fetch user/role/start for all updated events
                (link_id,),  # Parameterize link_id
            )
            updated_events_info = cursor.fetchall()

            # Collect unique user and role IDs from the updated events for notification
            updated_user_ids = {ev["user_id"] for ev in updated_events_info}
            updated_role_ids = {ev["role_id"] for ev in updated_events_info}
            # Need original user and role IDs from event_data_list for notification context
            original_user_ids = {ev["user_id"] for ev in event_data_list}
            original_role_ids = {ev["role_id"] for ev in event_data_list}

            # Notification needs original and new user/role IDs
            all_affected_user_ids = original_user_ids.union(updated_user_ids)
            all_affected_role_ids = original_role_ids.union(updated_role_ids)

            # Prepare context for notification
            notification_context = {
                "team": event_summary["team"],
                "link_id": link_id,
                "update_details": audit_update_context,  # Provide details of the change
            }

            # Use minimum start time of linked events for notification context
            linked_events_min_start_for_notification = event_summary["start"]

            # Create notification using the same cursor
            create_notification(
                notification_context,
                event_summary["team_id"],  # Team ID
                all_affected_role_ids,  # All roles affected
                EVENT_EDITED,
                all_affected_user_ids,  # All users affected
                cursor,  # Pass the cursor
                start_time=linked_events_min_start_for_notification,  # Use minimum start time
            )

            # 9. Commit the transaction if all steps in the try block succeed
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            # Check for potential IntegrityError messages from subqueries (non-existent user, role)
            if "Column 'role_id' cannot be null" in err_msg:
                # This could happen if the provided 'role' name in data['role'] doesn't exist
                err_msg = f'New role "{data.get("role")}" not found'
            elif "Column 'user_id' cannot be null" in err_msg:
                # This could happen if the provided 'user' name in data['user'] doesn't exist
                err_msg = f'New user "{data.get("user")}" not found'
            # Add other potential IntegrityError checks if applicable
            else:
                # Generic fallback for other integrity errors
                err_msg = f"Database Integrity Error: {err_msg}"

            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e

        except (
            Exception
        ) as e:  # Catch any other unexpected exceptions during the transaction
            # The with statement handles rollback automatically.
            print(
                f"Error during linked event update transaction for link ID {link_id}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

    resp.status = HTTP_204  # Standard response for successful PUT with no body
