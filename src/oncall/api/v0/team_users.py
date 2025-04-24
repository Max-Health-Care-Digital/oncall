# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_201, HTTPError
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required
from ...utils import load_json_body
from .users import get_user_data

constraints = {"active": "`team`.`active` = %s"}


def on_get(req, resp, team):
    """
    Get list of usernames for all team members. A user is a member of a team when
    he/she is a team admin or a member of one of the team's rosters. Accepts an
    ``active`` parameter in the query string that filters inactive (deleted) teams.

    **Example request:**

    .. sourcecode:: http

        GET /api/v0/teams/team-foo/users   HTTP/1.1
        Content-Type: application/json

    **Example response:**

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            "jdoe",
            "asmith"
        ]
    """
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        cursor = connection.cursor()
        query = """SELECT `user`.`name` FROM `user`
                   JOIN `team_user` ON `team_user`.`user_id`=`user`.`id`
                   JOIN `team` ON `team`.`id`=`team_user`.`team_id`
                   WHERE `team`.`name`=%s"""

        # Use a list for query parameters
        query_params = [team]

        # Handle optional active filter
        # Use req.get_param_as_bool for robustness and check if it was provided
        active = req.get_param_as_bool("active")
        if active is not None:
            query += " AND `team`.`active` = %s"
            # Convert boolean to int (0 or 1) as expected by many DBs for boolean/TINYINT
            query_params.append(int(active))

        # Execute the query with parameters
        cursor.execute(query, query_params)

        # Fetch the data
        data = [r[0] for r in cursor]

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'data' list
    resp.text = json_dumps(data)


@login_required
def on_post(req, resp, team):
    """
    Add user to a team. Deprecated; used only for testing purposes.
    """
    check_team_auth(team, req)
    data = load_json_body(req)

    user_name = data.get("name")  # Use .get
    if not user_name:
        raise HTTPError(
            "422 Unprocessable Entity",
            "Missing Parameter",  # More specific error type
            "name missing for user",
        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # Insert into team_user table using parameterized values in subqueries
            cursor.execute(
                """INSERT INTO `team_user` (`team_id`, `user_id`)
                          VALUES (
                              (SELECT `id` FROM `team` WHERE `name`=%s),
                              (SELECT `id` FROM `user` WHERE `name`=%s)
                          )""",
                (team, user_name),  # Parameterize team name and user name
            )

            # Commit the transaction if the insert succeeds
            connection.commit()

            # Fetch user data for the response body *inside* the with block
            # Call get_user_data using the current connection and cursor (via dbinfo)
            # This reuses the active connection instead of opening a new one.
            user_details_for_response = get_user_data(
                None, {"name": user_name}, dbinfo=(connection, cursor)
            )[
                0
            ]  # Pass dbinfo=(connection, cursor)

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            # Check for specific IntegrityError messages
            if "Column 'user_id' cannot be null" in err_msg:
                err_msg = f"user '{user_name}' not found"
            elif "Column 'team_id' cannot be null" in err_msg:
                err_msg = f"team '{team}' not found"
            elif "Duplicate entry" in err_msg:
                err_msg = f"user '{user_name}' is already in team '{team}'"
            else:
                # Generic fallback for other integrity errors
                err_msg = f"Database Integrity Error: {err_msg}"

            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e
        # Any other exception raised in the try block will also trigger rollback and cleanup.
        # The finally block is no longer needed for close calls.

    resp.status = HTTP_201
    # Use the user details fetched inside the with block
    resp.text = json_dumps(user_details_for_response)
