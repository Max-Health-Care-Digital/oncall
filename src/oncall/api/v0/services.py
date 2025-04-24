# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_201, HTTPBadRequest, HTTPError  # Added HTTPBadRequest
from ujson import dumps as json_dumps

from ... import db
from ...auth import debug_only  # Assuming debug_only is a valid decorator
from ...utils import load_json_body

constraints = {
    "id": "`service`.`id` = %s",
    "id__eq": "`service`.`id` = %s",
    "id__ne": "`service`.`id` != %s",
    "id__lt": "`service`.`id` < %s",
    "id__le": "`service`.`id` <= %s",
    "id__gt": "`service`.`id` > %s",
    "id__ge": "`service`.`id` >= %s",
    "name": "`service`.`name` = %s",
    "name__eq": "`service`.`name` = %s",
    "name__contains": '`service`.`name` LIKE CONCAT("%%", %s, "%%")',
    "name__startswith": '`service`.`name` LIKE CONCAT(%s, "%%")',
    "name__endswith": '`service`.`name` LIKE CONCAT("%%", %s)',
}


def on_get(req, resp):
    """
    Find services, filtered by params

    :query id: id of the service
    :query id__eq: id of the service
    :query id__gt: id greater than
    :query id__ge: id greater than or equal
    :query id__lt: id less than
    :query id__le: id less than or equal
    :query name: service name
    :query name__eq: service name
    :query name__contains: service name contains param
    :query name__startswith: service name starts with param
    :query name__endswith: service name ends with param

    **Example request**

    .. sourcecode:: http

        GET /api/v0/services?name__startswith=service  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            "service-foo"
        ]
    """
    # Base query string template
    query = "SELECT `name` FROM `service`"

    # Build WHERE clause using parameterized query snippets and values
    where_params_snippets = []  # e.g., "`service`.`name` = %s"
    where_vals = []  # e.g., ["service-foo"]

    for (
        key,
        val,
    ) in req.params.items():  # Iterate through items to get key and value
        if key in constraints:
            where_params_snippets.append(
                constraints[key]
            )  # Add the snippet with placeholder
            where_vals.append(val)  # Add the value

    # Combine WHERE clause snippets
    where_query = " AND ".join(where_params_snippets)

    # Final query string template
    if where_query:
        # Note: While this string formatting works with the parameters passed later,
        # building the full query template with placeholders directly is often cleaner.
        # Sticking to original style for minimal change.
        query = f"{query} WHERE {where_query}"

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a standard cursor
        cursor = connection.cursor()

        # Execute the query with the parameters
        # where_vals list will be empty if no constraints were applied
        cursor.execute(query, where_vals)

        # Fetch the data
        data = [r[0] for r in cursor]

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'data' list
    resp.text = json_dumps(data)


@debug_only
def on_post(req, resp):
    # Assuming debug_only decorator handles auth/access control
    data = load_json_body(req)

    service_name = data.get("name")  # Use .get
    if not service_name:
        raise HTTPBadRequest(
            "Missing Parameter", "name attribute missing from request body"
        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # Insert into service table using dictionary parameter
            cursor.execute(
                "INSERT INTO `service` (`name`) VALUES (%(name)s)",
                {"name": service_name},
            )  # Pass as dict

            # Commit the transaction if the insert succeeds
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback.
            err_msg = str(e.args[1])
            # Check for duplicate entry error
            if "Duplicate entry" in err_msg:
                err_msg = f'service name "{service_name}" already exists'  # Use f-string
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
                f"Error during service creation for name={service_name}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_201
