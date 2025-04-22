# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_user_auth, login_required
from ...utils import load_json_body


def on_get(req, resp, user_name):
    """
    Get all pinned team names for a user

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/users/jdoe/pinned_teams HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            "team-foo"
        ]
    """
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a cursor from the connection wrapper within the 'with' block
        cursor = connection.cursor()
        cursor.execute(
            """SELECT `team`.`name`
                          FROM `pinned_team` JOIN `team` ON `pinned_team`.`team_id` = `team`.`id`
                          WHERE `pinned_team`.`user_id` = (SELECT `id` FROM `user` WHERE `name` = %s)""",
            user_name,
        )
        teams = [r[0] for r in cursor]
        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit cursor.close() and connection.close() are no longer needed.

    resp.body = json_dumps(teams)


@login_required
def on_post(req, resp, user_name):
    """
    Pin a team to the landing page for a user

    **Example request**:

    .. sourcecode:: http

        POST /api/v0/users/jdoe/pinned_teams HTTP/1.1
        Host: example.com

        {
            "team": "team-foo"
        }

    :statuscode 201: Successful team pin
    :statuscode 400: Missing team parameter or team already pinned
    :statuscode 422: User or team not found
    """
    check_user_auth(user_name, req)
    data = load_json_body(req)
    team = data.get("team")
    if team is None:
        raise HTTPBadRequest("Invalid team pin", "Missing team parameter")

    # Use the 'with' statement for safe connection and transaction management
    # The ContextualRawConnection will handle rollback if an exception occurs
    # within the 'with' block and commit if it completes without exception.
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """INSERT INTO `pinned_team` (`user_id`, `team_id`)
                              VALUES ((SELECT `id` FROM `user` WHERE `name` = %s),
                                      (SELECT `id` FROM `team` WHERE `name` = %s))""",
                (user_name, team),
            )
            # Commit the transaction explicitly on success
            connection.commit()
        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block. We just need to handle
            # the specific errors and potentially re-raise.
            # Duplicate key (team already pinned)
            if e.args[0] == 1062:
                raise HTTPBadRequest(
                    "Invalid team pin", "Team already pinned for this user"
                )
            # Team/user is null (user or team not found)
            elif e.args[0] == 1048:
                err_msg = str(e.args[1])
                if err_msg == "Column 'user_id' cannot be null":
                    err_msg = 'user "%s" not found' % user_name
                elif err_msg == "Column 'team_id' cannot be null":
                    err_msg = 'team "%s" not found' % team
                # Changed status to 422 as per common API practice for unprocessable entities
                # when data lookup fails due to invalid input values (user/team name)
                raise HTTPError(
                    "422 Unprocessable Entity", "IntegrityError", err_msg
                )
            else:
                # Re-raise any other IntegrityError
                raise
        # The connection and cursor are automatically closed/released by the 'with' statement
        # No need for a finally block to close connection/cursor anymore.

    resp.status = HTTP_201