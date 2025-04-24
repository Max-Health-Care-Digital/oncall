# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import db


def on_get(req, resp):
    """
    Returns all notification types and whether they are reminder notifications.

    **Example request:**

    .. sourcecode:: http

        GET /api/v0/notification_types HTTP/1.1
        Host: example.com

    **Example response:**

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "name": "oncall_reminder",
                "is_reminder": 1
            }
        ]
    """
    data = []
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Ensure DictCursor is available
        if not db.DictCursor:
            raise RuntimeError(
                "DictCursor is required but not available. Check DBAPI driver and db.init()."
            )
        cursor = connection.cursor(db.DictCursor)

        cursor.execute("SELECT `name`, `is_reminder` FROM `notification_type`")
        # Fetch all data within the 'with' block
        data = cursor.fetchall()

        # No need for explicit cursor.close() or connection.close()
        # Connection is automatically released when exiting the 'with' block

    # Set the response text after the connection is closed
    resp.text = json_dumps(data)
