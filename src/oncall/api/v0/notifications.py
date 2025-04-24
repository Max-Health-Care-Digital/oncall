# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
from ujson import dumps as json_dumps

from ... import db

columns = {
    "id": "`notification`.`id` = %s",
    "event_id": "`notification`.`event_id` = %s",
    "active": "`notification`.`active` = %s",
}


def on_get(req, resp):
    """
    Find notifications, filtered by params

    :query id: id of the notification
    :query event_id: id of the associated event
    :query active: whether the notification is active (1) or inactive (0)

    **Example request**

    .. sourcecode:: http

        GET /api/v0/notifications?active=1  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "id": 1,
                "event_id": 1234,
                "user_id": 5678,
                "notification_mode_id": 1,
                "destination": "jdoe@example.com",
                "active": 1
            }
        ]
    """
    # Base query string template - Select all columns for full notification data
    # Adjusting SELECT * to list columns if possible, but * is often used for audit/notification tables
    # Let's stick to SELECT * based on the original query structure.
    query = "SELECT * FROM `notification`"

    # Build WHERE clause using parameterized query snippets and values
    where_params_snippets = []  # e.g., "`notification`.`id` = %s"
    where_vals = []  # e.g., [123]

    # Iterate through request parameters and build constraints
    for col in req.params:
        val = req.get_param(col)
        if col in columns:
            where_params_snippets.append(
                columns[col]
            )  # Add the snippet with placeholder
            where_vals.append(val)  # Add the value
        # else: Ignore unknown parameters

    # Combine WHERE clause snippets
    where_query = " AND ".join(
        where_params_snippets
    )  # Use AND as implied by separate constraints

    # Final query string template
    if where_query:
        # Note: While this string formatting works with the parameters passed later,
        # building the full query template with placeholders directly is often cleaner.
        # Sticking to original style for minimal change.
        query = f"{query} WHERE {where_query}"

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the query with the parameters
        # where_vals list will be empty if no constraints were applied
        cursor.execute(query, where_vals)

        # Fetch the data
        data = cursor.fetchall()

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'data' list
    resp.text = json_dumps(data)
