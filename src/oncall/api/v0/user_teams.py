# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTPNotFound
from ujson import dumps

from ... import db


def on_get(req, resp, user_name):
    """
    Get active teams by user name. Note that this does not return any deleted teams that
    this user is a member of.

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/users/jdoe/teams  HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            "team-foo",
            "team-bar"
        ]
    """
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        cursor = connection.cursor()
        # Execute the first query to get the user ID
        cursor.execute("SELECT `id` FROM `user` WHERE `name` = %s", user_name)

        # Check rowcount within the with block
        if cursor.rowcount < 1:
            # Raise HTTPNotFound within the with block. The context manager
            # will handle closing the connection even when an exception is raised.
            raise HTTPNotFound(description=f"User '{user_name}' not found")

        # Fetch the user ID
        user_id = cursor.fetchone()[0]

        # Execute the second query using the fetched user_id
        cursor.execute(
            """SELECT `team`.`name` FROM `team`
                          JOIN `team_user` ON `team_user`.`team_id` = `team`.`id`
                          WHERE `team_user`.`user_id` = %s AND `team`.`active` = TRUE""",
            user_id,
        )

        # Fetch the data into a list
        data = [r[0] for r in cursor]

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit cursor.close() and connection.close() are no longer needed.

    # Continue processing outside the with block using the fetched 'data'
    resp.text = dumps(data)
