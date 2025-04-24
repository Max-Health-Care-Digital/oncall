# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import (
    HTTP_204,
    HTTPBadRequest,  # Added HTTP_204, HTTPBadRequest
    HTTPError,
    HTTPNotFound,
)
from ujson import dumps

from ... import db
from ...auth import debug_only  # Assuming debug_only is a valid decorator
from ...utils import load_json_body


def on_get(req, resp, service):
    """
    Get service id and name by name

    **Example request**

    .. sourcecode:: http

        GET /api/v0/services/service-foo  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "id": 1234,
            "name": "service-foo"
        }

    """
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the query with the parameterized service name
        cursor.execute(
            "SELECT `id`, `name` FROM `service` WHERE `name`=%s",
            (service,),  # Parameterize service name as a tuple
        )

        # Fetch the single result
        data = cursor.fetchone()  # Use fetchone directly

        # Check if data was found within the with block
        if not data:  # fetchone returns None if no rows found
            raise HTTPNotFound(description=f"Service '{service}' not found")

        # No need to unpack into [service] if fetchone is used

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block
    resp.text = dumps(data)  # Use the fetched data dictionary directly


@debug_only
def on_put(req, resp, service):
    """
    Change name for a service. Currently unused/debug only.
    """
    # Assuming debug_only decorator handles auth/access control
    data = load_json_body(req)

    new_service_name = data.get("name")  # Use .get
    if not new_service_name:
        raise HTTPBadRequest(
            "Missing Parameter", "name attribute missing from request body"
        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # Execute the UPDATE query using parameterized values
            cursor.execute(
                "UPDATE `service` SET `name`=%s WHERE `name`=%s",
                (
                    new_service_name,
                    service,
                ),  # Parameterize new name and original name
            )
            updated_count = cursor.rowcount  # Store the number of rows updated

            # Check if any rows were updated *immediately after* this operation
            # If updated_count == 0, the service wasn't found by its original name
            if updated_count == 0:
                # Raise HTTPNotFound within the with block
                # This ensures the context manager handles connection cleanup and rollback.
                raise HTTPNotFound(
                    description=f"Service '{service}' not found for update"
                )

            # If rows were updated, commit the transaction
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback.
            err_msg = str(e.args[1])
            # Check for duplicate entry error
            if "Duplicate entry" in err_msg:
                err_msg = f'service name "{new_service_name}" already exists'  # Use f-string
            # Add other potential IntegrityError checks if applicable
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
                f"Error during service name update for '{service}': {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_204  # Standard response for successful PUT with no body


@debug_only
def on_delete(req, resp, service):
    """
    Delete a service. Currently unused/debug only.
    """
    # Assuming debug_only decorator handles auth/access control

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # FIXME: also delete team service mappings?
            # Original comment remains - need to consider foreign key constraints
            # or explicitly delete related records (e.g., from team_service) first.

            # Execute the DELETE query using parameterized value
            cursor.execute(
                "DELETE FROM `service` WHERE `name`=%s", (service,)
            )  # Parameterize service name as a tuple
            deleted_count = cursor.rowcount  # Store the number of rows deleted

            # Check if any rows were deleted *immediately after* this operation
            # If deleted_count == 0, the service wasn't found
            if deleted_count == 0:
                # Raise HTTPNotFound within the with block
                # This ensures the context manager handles connection cleanup and rollback.
                raise HTTPNotFound(
                    description=f"Service '{service}' not found for deletion"
                )

            # If rows were deleted, commit the transaction
            connection.commit()

        except Exception as e:  # Catch any exceptions during the transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error during service deletion for '{service}': {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement for close.

    # If the transaction was successful (committed), return 204 No Content
    resp.status = HTTP_204  # Standard response for successful DELETE
