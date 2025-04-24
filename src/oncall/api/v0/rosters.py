# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required
from ...constants import ROSTER_CREATED
from ...utils import create_audit, invalid_char_reg, load_json_body

# Assuming get_schedules is refactored to optionally use a provided connection/cursor (via dbinfo)
# or handle its own connection correctly when none is provided.
from .schedules import get_schedules

constraints = {
    "name": "`roster`.`name` = %s",
    "name__eq": "`roster`.`name` = %s",
    "name__contains": '`roster`.`name` LIKE CONCAT("%%", %s, "%%")',
    "name__startswith": '`roster`.`name` LIKE CONCAT(%s, "%%")',
    "name__endswith": '`roster`.`name` LIKE CONCAT("%%", %s)',
    "id": "`roster`.`id` = %s",
    "id__eq": "`roster`.`id` = %s",
    # Assuming other potential constraints are handled by the logic below
}


def get_roster_by_team_id(cursor, team_id, params=None):
    """
    Helper function to get roster data for a team. Uses the provided cursor.
    Calls get_schedules and passes the existing connection/cursor.

    :param cursor: An active database cursor.
    :param team_id: The ID of the team.
    :param params: Optional filter parameters for the roster query.
    :return: dict mapping roster names to their data (users, schedules, id).
    """
    # Ensure we have a connection object from the cursor
    connection = cursor.connection
    if connection is None:
        # This indicates the cursor is not linked to an active connection, which shouldn't happen
        # if it was passed correctly, but worth a check.
        raise ValueError("Provided cursor is not attached to a connection.")

    # get all rosters for a team
    # *** Cleaner parameterized query construction ***
    query = "SELECT `id`, `name` from `roster`"
    where_params_snippets = []
    where_values = []
    if params:
        # Assuming params keys are valid constraint keys for the roster table itself if needed
        # This part depends on what 'params' is expected to filter *rosters* by.
        # If it's only for schedules, this block is unused for the roster query.
        # Let's assume params are only for the schedules query for now as per original structure implicitly
        # If roster filters are needed, the constraints dict needs keys for roster fields.
        pass  # No roster-specific params handled here based on the original

    # Always filter by team_id for the initial roster fetch
    # The constraints dict provided seems geared towards schedule filters.
    # Assuming team_id is a direct filter on the roster table here.
    where_params_snippets.append("`roster`.`team_id` = %s")
    where_values.append(team_id)

    where_clause = " AND ".join(where_params_snippets)
    # Construct the final query string template
    final_query = f"{query} WHERE {where_clause}"

    print(f"get_roster_by_team_id: Roster query (template): {final_query}")
    print(f"get_roster_by_team_id: Roster query (values): {where_values}")
    # Execute the query with parameters using the provided cursor
    cursor.execute(final_query, where_values)

    # Fetch roster names and ids to initialize the dictionary
    rosters = {}
    roster_rows = cursor.fetchall()  # Fetch all roster rows first
    for row in roster_rows:
        rosters[row["name"]] = {"users": [], "schedules": [], "id": row["id"]}

    # If no rosters found, return early
    if not rosters:
        print("get_roster_by_team_id: No rosters found for team.")
        return {}

    # get users for each roster
    # Use IN clause filter on roster IDs found
    roster_ids = tuple(
        r["id"] for r in rosters.values()
    )  # Get IDs from the rosters found

    query_users = """SELECT `roster`.`name` AS `roster`,
                      `user`.`name` AS `user`,
                      `roster_user`.`in_rotation` AS `in_rotation`
               FROM `roster_user`
               JOIN `roster` ON `roster_user`.`roster_id`=`roster`.`id`
               JOIN `user` ON `roster_user`.`user_id`=`user`.`id`
               WHERE `roster_user`.`roster_id` IN %s"""  # Filter on roster_user.roster_id is more direct

    print(
        f"get_roster_by_team_id: Roster users query (template): {query_users}"
    )
    print(
        f"get_roster_by_team_id: Roster users query (values): {(roster_ids,)}"
    )

    # Execute the users query using the provided cursor and parameterized IN clause
    cursor.execute(
        query_users, (roster_ids,)
    )  # Pass as a tuple containing the tuple of IDs

    # Populate users for each roster
    user_rows = cursor.fetchall()  # Fetch all user rows
    for row in user_rows:
        if (
            row["roster"] in rosters
        ):  # Defensive check - roster name should exist
            rosters[row["roster"]]["users"].append(
                {"name": row["user"], "in_rotation": bool(row["in_rotation"])}
            )
        else:
            # This shouldn't happen if the join and ID filtering are correct, but can log
            print(
                f"Warning: Roster user found for unexpected roster name: {row['roster']}"
            )

    # get all schedules for the team by CALLING get_schedules
    # *** Pass the existing connection and cursor via dbinfo ***
    schedule_data = get_schedules(
        filter_params={"team_id": team_id},
        dbinfo=(connection, cursor),  # Pass the existing connection and cursor
        fields=None,  # Or specify the fields needed for schedules if not all
    )
    print(
        f"get_roster_by_team_id: Received {len(schedule_data)} schedules from get_schedules."
    )

    # Populate schedules for each roster from the schedule_data
    for schedule in schedule_data:
        # Check if 'roster' key exists and the roster name is in our dictionary
        # 'roster' key should exist if the 'roster' field was selected in get_schedules
        if "roster" in schedule and schedule["roster"] in rosters:
            rosters[schedule["roster"]]["schedules"].append(schedule)
        # else: schedule belongs to the team but not one of the filtered rosters (e.g. a schedule with no roster_id, though FK usually prevents this)

    # No close() needed here, the connection/cursor are managed by the caller of get_roster_by_team_id.
    return rosters


def on_get(req, resp, team):
    """
    Get roster info for a team. Returns a JSON object with roster names
    as keys, and info as values. This info includes the roster id, any
    schedules associated with the rosters, and roster users (along
    with their status as in/out of rotation).

    ... (docstring remains the same) ...
    """
    team_name = unquote(team)  # Renamed variable

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the query to get the team ID
        cursor.execute(
            "SELECT `id` FROM `team` WHERE `name`=%s", (team_name,)
        )  # Parameterize team_name

        # Check results and raise HTTPNotFound within the with block
        # Using rowcount != 1 check from original code
        if cursor.rowcount == 0:  # Team not found
            raise HTTPError(
                "422 Unprocessable Entity",  # Keep original status code
                "IntegrityError",  # Keep original type
                f'team "{team_name}" not found',  # Use f-string for clarity
            )
        # If rowcount > 1, something is wrong, but fetchone will get the first
        # The original code didn't explicitly handle > 1, so we won't add that check here.

        # Fetch the team ID
        team_id = cursor.fetchone()["id"]

        # Call get_roster_by_team_id using the cursor from this connection
        rosters = get_roster_by_team_id(cursor, team_id, req.params)

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched and processed 'rosters' dictionary
    resp.text = json_dumps(rosters)


@login_required
def on_post(req, resp, team):
    """
    Create a roster for a team

    ... (docstring remains the same) ...
    """
    team_name = unquote(team)  # Renamed variable
    data = load_json_body(req)

    roster_name = data.get("name")  # Use .get
    if not roster_name:
        raise HTTPBadRequest(
            "Missing Parameter", "name attribute missing or empty from request"
        )

    invalid_char = invalid_char_reg.search(roster_name)
    if invalid_char:
        raise HTTPBadRequest(
            "invalid roster name",
            f'roster name contains invalid character "{invalid_char.group()}"',  # Use f-string
        )

    check_team_auth(team_name, req)  # Use team_name

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        # Acquire a standard cursor (original used standard cursor)
        cursor = connection.cursor()  # Using standard cursor as in original

        try:
            # Insert into roster table
            cursor.execute(
                """INSERT INTO `roster` (`name`, `team_id`)
                          VALUES (%s, (SELECT `id` FROM `team` WHERE `name`=%s))""",
                (
                    roster_name,
                    team_name,
                ),  # Parameterize roster_name and team_name
            )

            # Create audit trail entry using the same cursor
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit(
                {"roster_id": cursor.lastrowid, "request_body": data},
                team_name,  # Use team_name
                ROSTER_CREATED,
                req,
                cursor,
            )

            # Commit the transaction if both insert and audit succeed
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            # Check for specific IntegrityError messages
            if "Duplicate entry" in err_msg:
                # Duplicate roster name within the team
                err_msg = f'roster name "{roster_name}" already exists for team "{team_name}"'
            elif "Column 'team_id' cannot be null" in err_msg:
                # Team not found in the subquery
                err_msg = f'team "{team_name}" not found'
            else:
                # Generic fallback for other integrity errors
                err_msg = f"Database Integrity Error: {err_msg}"

            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e
        # Do not need a finally block to close connection/cursor; the 'with' statement handles it.
        # Any other exception raised in the try block will also trigger rollback and cleanup.

    resp.status = HTTP_201
