# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTPBadRequest, HTTPError, HTTPNotFound, HTTP_204
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required
from ...constants import ROSTER_DELETED, ROSTER_EDITED
from ...utils import create_audit, invalid_char_reg, load_json_body
# Assuming get_schedules is refactored to optionally use a provided connection/cursor (via dbinfo)
# or handle its own connection correctly when none is provided.
from .schedules import get_schedules


def on_get(req, resp, team, roster):
    """
    Get user and schedule info for a roster

    ... (docstring remains the same) ...
    """
    team_name, roster_name = unquote(team), unquote(roster) # Renamed variables

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # 1. Get roster and team IDs
        cursor.execute(
            """SELECT `roster`.`id` AS `roster_id`, `team`.`id` AS `team_id` FROM `roster`
                      JOIN `team` ON `team`.`id`=`roster`.`team_id`
                      WHERE `team`.`name`=%s AND `roster`.`name`=%s""",
            (team_name, roster_name), # Parameterize team_name and roster_name
        )
        results = cursor.fetchall()

        # Check if roster was found within the with block
        if not results:
            raise HTTPNotFound(description=f"Roster '{roster_name}' not found for team '{team_name}'")
        [ids_info] = results # Unpack the single result
        team_id = ids_info["team_id"]
        roster_id = ids_info["roster_id"]

        # 2. Get list of users in the roster using the same cursor
        cursor.execute(
            """SELECT `user`.`name` as `name`,
                             `roster_user`.`in_rotation` AS `in_rotation`,
                             `roster_user`.`roster_priority`
                      FROM `roster_user`
                      JOIN `user` ON `roster_user`.`user_id`=`user`.`id`
                      WHERE `roster_user`.`roster_id`=%s""",
            (roster_id,), # Parameterize roster_id
        )
        users = [user for user in cursor]

        # 3. Get list of schedules in the roster using get_schedules
        # Pass the connection and cursor via dbinfo for reuse
        schedules = get_schedules({"roster_id": roster_id}, dbinfo=(connection, cursor)) # Filter by roster_id here


        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched data
    # Structure the response body
    response_body = {
        roster_name: { # Key by roster name
             "id": roster_id,
             "users": users,
             "schedules": schedules
        }
    }
    resp.text = json_dumps(response_body)


@login_required
def on_put(req, resp, team, roster):
    """
    Change roster name. Must have team admin privileges.

    ... (docstring remains the same) ...
    """
    team_name, roster_name = unquote(team), unquote(roster) # Renamed variables
    data = load_json_body(req)
    new_roster_name = data.get("name")
    roster_order_list = data.get("roster_order") # Renamed to clarify it's a list

    check_team_auth(team_name, req) # Use team_name

    # Validate input data
    if not (new_roster_name is not None or roster_order_list is not None):
        # Changed condition to check if *neither* is provided (allowing one or both)
        raise HTTPBadRequest(
            "Invalid roster update", "Missing roster name or roster_order in request body"
        )

    if new_roster_name is not None and new_roster_name == "": # Check for empty string if name provided
         raise HTTPBadRequest("Invalid roster name", "Empty roster name provided")


    # Use the 'with' statement for safe connection and transaction management
    # The entire PUT operation should be one transaction
    with db.connect() as connection:
        cursor = connection.cursor() # Use standard cursor

        # 1. Get roster and team IDs and validate existence early
        cursor.execute(
            """SELECT `roster`.`id` AS `roster_id`, `team`.`id` AS `team_id` FROM `roster`
                      JOIN `team` ON `team`.`id`=`roster`.`team_id`
                      WHERE `team`.`name`=%s AND `roster`.`name`=%s""",
            (team_name, roster_name), # Parameterize team_name and roster_name
        )
        ids_info = cursor.fetchone() # Use fetchone

        # Check if roster was found within the with block
        if not ids_info:
            raise HTTPNotFound(description=f"Roster '{roster_name}' not found for team '{team_name}' for update")

        print(f"{ids_info = }")
        roster_id = ids_info["roster_id"]
        team_id = ids_info["team_id"] # Get team_id as well

        try:
            # 2. Handle roster order update if provided
            if roster_order_list is not None:
                # Ensure roster_order_list is a list
                if not isinstance(roster_order_list, list):
                     raise HTTPBadRequest("Invalid roster order", "roster_order must be a list")


                # Get current roster users to validate provided order
                cursor.execute(
                    """SELECT `user`.`name` FROM `roster_user`
                                  JOIN `user` ON `roster_user`.`user_id` = `user`.`id`
                                  WHERE `roster_user`.`roster_id` = %s""",
                    (roster_id,), # Parameterize roster_id
                )
                current_roster_users = {row[0] for row in cursor.fetchall()} # Fetch all user names

                # Validate provided roster_order_list against current users
                if not all(user in current_roster_users for user in roster_order_list):
                    raise HTTPBadRequest(
                        "Invalid roster order",
                        "All users in provided order must be part of the roster",
                    )
                if len(roster_order_list) != len(current_roster_users):
                    raise HTTPBadRequest(
                        "Invalid roster order",
                        "Roster order must include all current roster members",
                    )

                # Prepare parameters for executemany update
                order_update_params = [
                    (idx, roster_id, user) # Update roster_priority, WHERE roster_id, WHERE user_name
                    for idx, user in enumerate(roster_order_list)
                ]

                # Execute the batch update for roster_priority
                # Use %s placeholders for the UPDATE statement
                cursor.executemany(
                    """UPDATE roster_user SET roster_priority = %s
                                      WHERE roster_id = %s
                                      AND user_id = (SELECT id FROM user WHERE name = %s)""",
                    order_update_params,
                )

            # 3. Handle roster name update if provided and different
            if new_roster_name is not None and new_roster_name != roster_name:
                # Validate the new name (invalid chars already checked)
                invalid_char = invalid_char_reg.search(new_roster_name) # Re-check just in case
                if invalid_char:
                    raise HTTPBadRequest(
                        "Invalid roster name",
                        f'Roster name contains invalid character "{invalid_char.group()}"',
                    )

                # Execute the UPDATE query for the roster name
                cursor.execute(
                    """UPDATE `roster` SET `name`=%s
                       WHERE `id`=%s""", # Update by roster_id, not name+team_id for safety
                    (new_roster_name, roster_id), # Parameterize new_roster_name and roster_id
                )

                # Create audit trail entry for name change
                create_audit(
                    {"old_name": roster_name, "new_name": new_roster_name}, # Use original and new names
                    team_name, # Use team_name
                    ROSTER_EDITED,
                    req,
                    cursor, # Pass the cursor
                )
            # Note: Audit for roster order change might be needed too, but original code didn't have it.

            # 4. Commit the transaction if any updates or audit occurred successfully
            # This commit is inside the try block and covers all operations performed.
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            # Check for duplicate entry error, likely from the roster name update
            if "Duplicate entry" in err_msg:
                err_msg = f"Roster '{new_roster_name}' already exists for team '{team_name}'"
            else:
                 # Generic fallback for other integrity errors
                 err_msg = f"Database Integrity Error: {err_msg}"

            # Re-raise the exception after formatting the error message
            raise HTTPError("422 Unprocessable Entity", "IntegrityError", err_msg) from e
        # Any other exception raised in the try block will also trigger rollback and cleanup.
        # The finally block is no longer needed for close calls.

    resp.status = HTTP_204 # Standard response for successful PUT with no response body


@login_required
def on_delete(req, resp, team, roster):
    """
    Delete roster
    """
    team_name, roster_name = unquote(team), unquote(roster) # Renamed variables
    check_team_auth(team_name, req) # Use team_name

    # Use the 'with' statement for safe connection and transaction management
    # The entire delete operation should be one transaction
    with db.connect() as connection:
        cursor = connection.cursor()

        # 1. Check if roster exists and get its ID and team ID early
        cursor.execute("SELECT r.id, t.id FROM roster r JOIN team t ON r.team_id = t.id WHERE r.name = %s AND t.name = %s", (roster_name, team_name))
        roster_team_ids = cursor.fetchone()
        if not roster_team_ids:
            # Roster not found, raise 404 immediately within the with block
            # This ensures cleanup via the context manager.
            raise HTTPNotFound(description=f"Roster '{roster_name}' not found for team '{team_name}' for deletion")

        roster_id, team_id = roster_team_ids # Get IDs

        try:
            # 2. Get user IDs that were in this roster (needed for potential team_user deletion)
            # Fetch this list *before* deleting from roster_user
            cursor.execute("SELECT user_id FROM roster_user WHERE roster_id = %s", (roster_id,))
            user_ids_in_roster = [row[0] for row in cursor.fetchall()] # Fetch all user IDs

            # 3. Delete roster users for this specific roster ID
            cursor.execute("DELETE FROM roster_user WHERE roster_id = %s", (roster_id,))
            # The rowcount here tells us how many users were removed from the roster.

            # 4. If there were users in the roster, delete them from the team if they aren't in other rosters/admins
            if user_ids_in_roster:
                # Convert list to tuple for the IN clause
                user_ids_in_roster_tuple = tuple(user_ids_in_roster)

                # Construct and execute the DELETE FROM team_user query
                # Use parameterized queries throughout
                query_delete_team_user = """DELETE FROM team_user WHERE user_id IN %s AND user_id NOT IN
                                               (SELECT roster_user.user_id
                                                FROM roster_user JOIN roster ON roster.id = roster_user.roster_id
                                                WHERE team_id = %s
                                               UNION
                                               (SELECT user_id FROM team_admin
                                                WHERE team_id = %s))
                                           AND team_user.team_id = %s"""
                # Pass parameters: (user_ids tuple, team_id, team_id, team_id)
                cursor.execute(query_delete_team_user, (user_ids_in_roster_tuple, team_id, team_id, team_id))

            # 5. Delete the roster itself
            # Delete by ID for robustness, not name + team name again
            cursor.execute("DELETE FROM roster WHERE id = %s", (roster_id,))
            roster_deleted_count = cursor.rowcount # Should be 1 if deletion by ID worked

            # Optional: Check if roster was deleted (should be 1 if we got here)
            # If it's 0, something went very wrong after finding it but before deleting.
            # if roster_deleted_count == 0:
            #     raise HTTPError("500 Internal Server Error", "Database Error", f"Failed to delete roster {roster_name} unexpectedly")

            # 6. Create audit trail entry
            create_audit({"name": roster_name}, team_name, ROSTER_DELETED, req, cursor) # Use variable names, pass cursor

            # 7. Commit the transaction if all steps succeed
            connection.commit()

        except Exception as e: # Catch potential exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(f"Error during roster delete transaction for {team_name}/{roster_name}: {e}") # Replace with logging
            raise # Re-raise the exception for Falcon

        # Do not need to close connection/cursor; the 'with' statement handles it upon exiting the block.

    resp.status = HTTP_204 # Standard response for successful DELETE