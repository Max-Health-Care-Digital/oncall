# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from urllib.parse import unquote

from falcon import HTTP_204  # Added HTTP_204 for successful delete
from falcon import HTTPNotFound

from ... import db
from ...auth import check_team_auth, login_required
from ...constants import ADMIN_DELETED
from ...utils import create_audit, unsubscribe_notifications


@login_required
def on_delete(req, resp, team, user):
    """
    Delete team admin user. Removes admin from the team if he/she is not a member of any roster.

    **Example request:**

    .. sourcecode:: http

        DELETE /api/v0/teams/team-foo/admins/jdoe HTTP/1.1

    :statuscode 200: Successful delete
    :statuscode 404: Team admin not found
    """
    team_name = unquote(team)  # Renamed variable
    user_name = user  # Renamed variable (user is already unquoted by Falcon)

    check_team_auth(team_name, req)  # Use team_name

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # 1. Delete the user from the team_admin table
            # Execute the DELETE query using parameterized values
            cursor.execute(
                """DELETE FROM `team_admin`
                              WHERE `team_id`=(SELECT `id` FROM `team` WHERE `name`=%s)
                              AND `user_id`=(SELECT `id` FROM `user` WHERE `name`=%s)""",
                (team_name, user_name),  # Parameterize team_name and user_name
            )
            deleted_count = cursor.rowcount  # Store the number of rows deleted

            # Check if any rows were deleted *immediately after* this operation
            # If deleted_count == 0, the team admin mapping wasn't found
            if deleted_count == 0:
                # Raise HTTPNotFound within the with block
                # This ensures the context manager handles connection cleanup and rollback.
                raise HTTPNotFound(
                    description=f"Admin user '{user_name}' not found for team '{team_name}'"
                )

            # 2. Create audit trail entry using the same cursor
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit(
                {"user": user_name}, team_name, ADMIN_DELETED, req, cursor
            )  # Use renamed variables, pass cursor

            # 3. Remove user from the team_user table if needed (if not in other rosters/admins)
            # Execute the DELETE query using parameterized values
            query_delete_team_user = """DELETE FROM `team_user` WHERE `user_id` = (SELECT `id` FROM `user` WHERE `name`=%s) AND `user_id` NOT IN
                                           (SELECT `roster_user`.`user_id`
                                            FROM `roster_user` JOIN `roster` ON `roster`.`id` = `roster_user`.`roster_id`
                                            WHERE team_id = (SELECT `id` FROM `team` WHERE `name`=%s)
                                           UNION
                                           (SELECT `user_id` FROM `team_admin`
                                            WHERE `team_id` = (SELECT `id` FROM `team` WHERE `name`=%s)))
                                       AND `team_user`.`team_id` = (SELECT `id` FROM `team` WHERE `name` = %s)"""
            cursor.execute(
                query_delete_team_user,
                (user_name, team_name, team_name, team_name),
            )  # Parameterize user_name and team_name

            # Check if the user was removed from team_user (optional logic check)
            if cursor.rowcount != 0:
                # 4. Unsubscribe user from notifications if they were removed from team_user
                # Assuming unsubscribe_notifications takes a cursor and handles DB ops within it
                unsubscribe_notifications(
                    team_name, user_name, cursor
                )  # Use renamed variables, pass cursor

            # 5. Commit the transaction if all steps succeed
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during team admin deletion for team={team_name}, user={user_name}: {e}"
            )  # Replace with logging
            # Re-raise the exception for Falcon to handle (e.g., translate DB errors to 500)
            raise

        # Do not need finally block; rely on the 'with' statement for close.

    # If the transaction was successful (committed), return 204 No Content
    resp.status = HTTP_204  # Standard response for successful DELETE
