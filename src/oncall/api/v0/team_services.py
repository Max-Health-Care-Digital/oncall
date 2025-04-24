# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTP_201, HTTPBadRequest, HTTPError  # Added HTTPBadRequest
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required
from ...utils import load_json_body


def on_get(req, resp, team):
    """
    Get list of services mapped to a team

    **Example request**:

    .. sourcecode:: http

        GET /api/v0/teams/team-foo/services  HTTP/1.1
        Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            "service-foo",
            "service-bar"
        ]
    """
    team_name = unquote(team)  # Renamed variable

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a standard cursor
        cursor = connection.cursor()
        cursor.execute(
            """SELECT `service`.`name` FROM `service`
                          JOIN `team_service` ON `team_service`.`service_id`=`service`.`id`
                          JOIN `team` ON `team`.`id`=`team_service`.`team_id`
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
    Create team to service mapping. Takes an object defining "name", then maps
    that service to the team specified in the URL. Note that this endpoint does
    not create a service; it expects this service to already exist.

    ... (docstring remains the same) ...
    """
    team_name = unquote(team)  # Renamed variable
    check_team_auth(team_name, req)  # Use team_name
    data = load_json_body(req)

    service_name = data.get("name")  # Use .get
    if not service_name:
        raise HTTPBadRequest(
            "Missing Parameter", 'missing field "name" in request body'
        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # 1. Check if the service is already claimed by *any* team
            # (Based on original logic; potential ambiguity with desired relationship - N:M vs 1:N service->team)
            # Execute the SELECT query using parameterized value
            cursor.execute(
                """SELECT `team`.`name` from `team_service`
                              JOIN `team` ON `team`.`id` = `team_service`.`team_id`
                              JOIN `service` ON `service`.`id` = `team_service`.`service_id`
                              WHERE `service`.`name` = %s""",
                (service_name,),  # Parameterize service_name
            )
            claimed_teams = [
                r[0] for r in cursor.fetchall()
            ]  # Fetch all results, just in case

            if claimed_teams:
                # If the service is already mapped to at least one team
                # Original code raised an error if claimed by ANY team.
                # If many-to-many is intended, this check needs refinement.
                # Sticking to original "claimed by team" logic for now.
                raise HTTPError(
                    "422 Unprocessable Entity",
                    "IntegrityError",
                    f'service "{service_name}" already claimed by team "{claimed_teams[0]}"',  # Use first claimed team name
                )

            # 2. Insert the team-service mapping
            # Execute the INSERT query using parameterized values in subqueries
            cursor.execute(
                """INSERT INTO `team_service` (`team_id`, `service_id`)
                              VALUES (
                                  (SELECT `id` FROM `team` WHERE `name`=%s),
                                  (SELECT `id` FROM `service` WHERE `name`=%s)
                              )""",
                (
                    team_name,
                    service_name,
                ),  # Parameterize team_name and service_name
            )

            # 3. Commit the transaction if both check and insert succeed
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback.
            err_msg = str(e.args[1])
            # Check for specific IntegrityError messages
            if "Column 'service_id' cannot be null" in err_msg:
                # This occurs if the service name in the subquery doesn't exist
                err_msg = f'service "{service_name}" not found'
            elif "Column 'team_id' cannot be null" in err_msg:
                # This occurs if the team name in the subquery doesn't exist
                err_msg = f'team "{team_name}" not found'
            elif "Duplicate entry" in err_msg:
                # This occurs if the team_id/service_id pair already exists (user tried to map same service to same team twice)
                err_msg = f'service "{service_name}" is already associated with team "{team_name}"'
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
                f"Error during team service mapping for team={team_name}, service={service_name}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_201
