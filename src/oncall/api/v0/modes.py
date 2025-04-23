# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import db


def on_get(req, resp):
    """
    Get all contact modes
    """
    data = []
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Create a standard cursor (not DictCursor as we access by index)
        cursor = connection.cursor()
        cursor.execute("SELECT `name` FROM `contact_mode`")
        # Fetch data within the 'with' block
        data = [row[0] for row in cursor]
        # No need for explicit cursor.close() or connection.close()
        # Connection is automatically released when exiting the 'with' block

    # Set the response text after the connection is closed
    resp.text = json_dumps(data)
