# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import db


def on_get(req, resp, team):
    """
    Get audit log entries for a specific team.

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/teams/team-foo/audit  HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "description":"User jdoe added to team team-foo",
                "timestamp": 1678886400,
                "owner_name": "admin_user",
                "action_name: "team_user_added"
            }
            ...
        ]

    """
    audit_query = """SELECT `audit_log`.`description`, `audit_log`.`timestamp`,
                            `audit_log`.`owner_name`, `audit_log`.`action_name`
                     FROM `audit_log` WHERE `team_name` = %s"""

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the query with the parameterized team name
        cursor.execute(
            audit_query, (team,)
        )  # Parameterize team name as a tuple

        # Fetch the data
        data = cursor.fetchall()

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'data' list
    resp.text = json_dumps(data)
