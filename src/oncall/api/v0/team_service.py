# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import (
    HTTP_204,  # Added HTTP_204 for successful delete
    HTTPNotFound,
)
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_team_auth, login_required


def on_get(req, resp):
    """
    Get list of team to service mappings

    **Example request**:

    .. sourcecode:: http

        GET /api/v0/team_services  HTTP/1.1
        Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "team": "team1",
                "service" : "service-foo"
            }
        ]
    """
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a standard cursor
        cursor = connection.cursor()
        query = """SELECT `team`.`name` as team_name, `service`.`name` as service_name FROM `team_service`
                          JOIN `service` ON `team_service`.`service_id`=`service`.`id`
                          JOIN `team` ON `team_service`.`team_id`=`team`.`id`"""

        # Execute the query (no parameters needed)
        cursor.execute(query)

        # Fetch the data
        data = [{"team": r[0], "service": r[1]} for r in cursor]

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'data' list
    resp.text = json_dumps(data)


@login_required
def on_delete(req, resp, team, service):
    """
    Delete service team mapping. Only allowed for team admins.

    **Example request**:

    .. sourcecode:: http

        GET /api/v0/team_services  HTTP/1.1
        Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "team": "team1",
                "service" : "service-foo"
            }
        ]
    """
    team_name = unquote(team)  # Renamed variable
    service_name = unquote(service)  # Renamed variable

    check_team_auth(team_name, req)  # Use team_name

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # Execute the DELETE query using parameterized values for names in subqueries
            cursor.execute(
                """DELETE FROM `team_service`
                              WHERE `team_id`=(SELECT `id` FROM `team` WHERE `name`=%s)
                              AND `service_id`=(SELECT `id` FROM `service` WHERE `name`=%s)""",
                (
                    team_name,
                    service_name,
                ),  # Parameterize team_name and service_name
            )
            deleted_count = cursor.rowcount  # Store the number of rows deleted

            # Check if any rows were deleted *immediately after* the DELETE operation
            # If deleted_count == 0, the mapping wasn't found
            if deleted_count == 0:
                # Raise HTTPNotFound within the with block
                # This ensures the context manager handles connection cleanup and rollback.
                raise HTTPNotFound(
                    description=f"Service '{service_name}' not found for team '{team_name}'"
                )

            # If rows were deleted, commit the transaction
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during team service deletion for team={team_name}, service={service_name}: {e}"
            )  # Replace with logging
            # Re-raise the exception for Falcon to handle (e.g., translate DB errors to 500)
            raise

        # Do not need finally block; rely on the 'with' statement for close.

    # If the transaction was successful (committed), return 204 No Content
    resp.status = HTTP_204  # Standard response for successful DELETE
