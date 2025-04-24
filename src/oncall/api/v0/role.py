# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_204  # Added HTTP_204 for successful delete
from falcon import HTTPNotFound

from ... import db
from ...auth import debug_only  # Assuming debug_only is a valid decorator


@debug_only
def on_delete(req, resp, role):
    # Assuming debug_only decorator handles auth/access control

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # TODO: also remove any schedule and event that references the role?
            # Original comment remains - need to consider foreign key constraints
            # or explicitly delete related records (e.g., schedules, events) first.

            # Execute the DELETE query using parameterized value
            cursor.execute(
                "DELETE FROM `role` WHERE `name`=%s", (role,)
            )  # Parameterize role name as a tuple
            deleted_count = cursor.rowcount  # Store the number of rows deleted

            # Check if any rows were deleted *immediately after* this operation
            # If deleted_count == 0, the role wasn't found
            if deleted_count == 0:
                # Raise HTTPNotFound within the with block
                # This ensures the context manager handles connection cleanup and rollback.
                raise HTTPNotFound(
                    description=f"Role '{role}' not found for deletion"
                )

            # If rows were deleted, commit the transaction
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during role deletion for '{role}': {e}"
            )  # Replace with logging
            raise  # Re-raise the exception for Falcon to handle (e.g., translate DB errors to 500)

        # Do not need finally block; rely on the 'with' statement for close.

    # If the transaction was successful (committed), return 204 No Content
    resp.status = HTTP_204  # Standard response for successful DELETE
