# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required
from ...constants import ADMIN_CREATED
from ...utils import create_audit, load_json_body, subscribe_notifications
from .users import get_user_data


def on_get(req, resp, team):
    """
    Get list of admin usernames for a team

    **Example request**

    .. sourcecode:: http

        GET /api/v0/teams/team-foo/admins  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            "jdoe",
            "asmith"
        ]
    """
    team_name = unquote(team)  # Renamed variable

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a standard cursor
        cursor = connection.cursor()
        cursor.execute(
            """SELECT `user`.`name` FROM `user`
                      JOIN `team_admin` ON `team_admin`.`user_id`=`user`.`id`
                      JOIN `team` ON `team`.`id`=`team_admin`.`team_id`
                      WHERE `team`.`name`=%s""",
            (team_name,),  # Parameterize team_name as a tuple
        )
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
    Add user as a team admin. Responds with that user's info (similar to user GET).
    Subscribes this user to default notifications for the team, and adds the user
    to the team (if needed).

    **Example request**

    .. sourcecode:: http

        POST /api/v0/teams/team-foo/admins  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "active": 1,
            "contacts": {
                "call": "+1 111-111-1111",
                "email": "jdoe@example.com",
                "im": "jdoe",
                "sms": "+1 111-111-1111"
            },
            "full_name": "John Doe",
            "id": 9535,
            "name": "jdoe",
            "photo_url": "image.example.com",
            "time_zone": "US/Pacific"
        }

    :statuscode 201: Successful admin added
    :statuscode 400: Missing name attribute in request
    :statuscode 422: Invalid team/user, or user is already a team admin
    """
    team_name = unquote(team)  # Renamed variable
    check_team_auth(team_name, req)  # Use team_name
    data = load_json_body(req)

    user_name = data.get("name")  # Use .get
    if not user_name:
        raise HTTPBadRequest(
            "Missing Parameter", "name attribute missing from request body"
        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        # 1. Get team_id and user_id using UNION ALL
        # Use parameterized queries with %s placeholders
        cursor.execute(
            """(SELECT `id` FROM `team` WHERE `name`=%s)
                      UNION ALL
                      (SELECT `id` FROM `user` WHERE `name`=%s)""",
            (team_name, user_name),  # Parameterize team_name and user_name
        )
        results = cursor.fetchall()  # Fetch all results

        # Check results count
        if len(results) < 2:
            # Determine which one was not found for a more specific error message
            team_exists = any(
                r[0] is not None for r in results[:1]
            )  # Check if first result is not None
            user_exists = any(
                r[0] is not None for r in results[1:]
            )  # Check if second result is not None

            if not team_exists and not user_exists:
                error_msg = (
                    f'team "{team_name}" and user "{user_name}" not found'
                )
            elif not team_exists:
                error_msg = f'team "{team_name}" not found'
            else:  # not user_exists
                error_msg = f'user "{user_name}" not found'

            raise HTTPError(
                "422 Unprocessable Entity",
                "IntegrityError",  # Keep original type
                error_msg,
            )

        # Unpack results - order is guaranteed by UNION ALL structure
        team_id = results[0][0]
        user_id = results[1][0]  # Assuming team_id comes first, then user_id

        try:
            # 2. Add user to the team if not already a member (INSERT IGNORE into team_user)
            # *** FIX: Use %s placeholders instead of unsafe %r ***
            cursor.execute(
                """INSERT IGNORE INTO `team_user` (`team_id`, `user_id`) VALUES (%s, %s)""",
                (team_id, user_id),  # Pass values as a tuple with %s
            )

            # 3. Add user as a team admin (INSERT into team_admin)
            # *** FIX: Use %s placeholders instead of unsafe %r ***
            cursor.execute(
                """INSERT INTO `team_admin` (`team_id`, `user_id`) VALUES (%s, %s)""",
                (team_id, user_id),  # Pass values as a tuple with %s
            )

            # 4. Subscribe user to team notifications using the same cursor
            # Assuming subscribe_notifications takes a cursor and handles DB ops within it
            subscribe_notifications(
                team_name, user_name, cursor
            )  # Use renamed variables

            # 5. Create audit trail entry using the same cursor
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit(
                {"user": user_name}, team_name, ADMIN_CREATED, req, cursor
            )  # Use renamed variables, pass cursor

            # 6. Commit the transaction if all steps succeed
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

            # 7. Fetch user data for the response body *inside* the with block
            # Call get_user_data using the current connection and cursor (via dbinfo)
            # This reuses the active connection instead of opening a new one.
            user_details_for_response = get_user_data(
                None, {"name": user_name}, dbinfo=(connection, cursor)
            )[
                0
            ]  # Pass dbinfo

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback.
            err_msg = str(e.args[1])
            # Check for specific IntegrityError messages
            if "Column 'team_id' cannot be null" in err_msg:
                # This indicates the team name lookup earlier failed, but the check already handles this.
                # Keeping as a defensive fallback.
                err_msg = (
                    f"team '{team_name}' not found (IntegrityError fallback)"
                )
            elif "Column 'user_id' cannot be null" in err_msg:
                # This indicates the user name lookup earlier failed, but the check already handles this.
                # Keeping as a defensive fallback.
                err_msg = (
                    f"user '{user_name}' not found (IntegrityError fallback)"
                )
            elif "Duplicate entry" in err_msg:
                # This occurs if the team_id/user_id pair already exists in team_admin
                err_msg = f"user '{user_name}' is already an admin of team '{team_name}'"
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
                f"Error during team admin creation for team={team_name}, user={user_name}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_201
    # Use the user details fetched inside the with block
    resp.text = json_dumps(user_details_for_response)
