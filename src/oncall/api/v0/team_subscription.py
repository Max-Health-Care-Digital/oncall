# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_204, HTTPNotFound
from ujson import dumps as json_dumps  # Imported but not used?

from ... import db
from ...auth import check_team_auth, login_required


@login_required
def on_delete(req, resp, team, subscription, role):
    # Use team and subscription names directly in check_team_auth as they are URL parameters
    check_team_auth(team, req)

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # Execute the DELETE query using parameterized values for names in subqueries
            cursor.execute(
                """DELETE FROM `team_subscription`
                              WHERE team_id = (SELECT `id` FROM `team` WHERE `name` = %s)
                              AND `subscription_id` = (SELECT `id` FROM `team` WHERE `name` = %s)\
                              AND `role_id` = (SELECT `id` FROM `role` WHERE `name` = %s)""",
                (
                    team,
                    subscription,
                    role,
                ),  # Parameterize team, subscription, and role names
            )
            deleted_count = cursor.rowcount  # Store the number of rows deleted

            # Check if any rows were deleted *immediately after* the DELETE operation
            # If deleted_count == 0, the subscription wasn't found
            if deleted_count == 0:
                # Raise HTTPNotFound within the with block
                # This ensures the context manager handles connection cleanup and rollback.
                raise HTTPNotFound(
                    description=f"Subscription for team '{team}', subscription team '{subscription}', and role '{role}' not found"
                )

            # If rows were deleted, commit the transaction
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during team subscription deletion for team={team}, sub={subscription}, role={role}: {e}"
            )  # Replace with logging
            # Re-raise the exception for Falcon to handle (e.g., translate DB errors to 500)
            raise

        # Do not need finally block; rely on the 'with' statement for close.

    # If the transaction was successful (committed), return 204 No Content
    resp.status = HTTP_204  # Standard response for successful DELETE
