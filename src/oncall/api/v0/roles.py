# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from collections import defaultdict

from falcon import HTTP_201, HTTPError
from ujson import dumps as json_dumps

from ... import db
from ...auth import debug_only
from ...utils import load_json_body

columns = {
    "id": "`role`.`id` as `id`",
    "name": "`role`.`name` as `name`",
    "display_order": "`role`.`display_order` as `display_order`",
}

all_columns = ", ".join(columns.values())

constraints = {
    "id": "`role`.`id` = %s",
    "id__eq": "`role`.`id` = %s",
    "id__ne": "`role`.`id` != %s",
    "id__lt": "`role`.`id` < %s",
    "id__le": "`role`.`id` <= %s",
    "id__gt": "`role`.`id` > %s",
    "id__ge": "`role`.`id` >= %s",
    "name": "`role`.`name` = %s",
    "name__eq": "`role`.`name` = %s",
    "name__contains": '`role`.`name` LIKE CONCAT("%%", %s, "%%")',
    "name__startswith": '`role`.`name` LIKE CONCAT(%s, "%%")',
    "name__endswith": '`role`.`name` LIKE CONCAT("%%", %s)',
}


# This function receives a cursor, it does NOT need to manage the connection
def get_role_ids(cursor, roles):
    if not roles:
        return []

    role_query = (
        "SELECT DISTINCT `id` FROM `role` WHERE `name` IN ({0})".format(
            ",".join(["%s"] * len(roles))
        )
    )
    # we need prepared statements here because roles come from user input
    cursor.execute(role_query, roles)
    # It's good practice to fetch all results if you need them outside the cursor's
    # immediate scope, though fetching all with fetchall() works too.
    # No close() needed here, the connection/cursor are managed by the caller.
    return [row["id"] for row in cursor]


def on_get(req, resp):
    """
    Role search.

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/roles?name__startswith=pri HTTP/1.1
       Host: example.com

    **Example response:**

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "id": 1,
                "name": "primary",
                "display_order": 1
            }
        ]

    :query id: id of the role
    :query id__eq: id of the role
    :query id__gt: id greater than
    :query id__ge: id greater than or equal
    :query id__lt: id less than
    :query id__le: id less than or equal
    :query name: role name
    :query name__eq: role name
    :query name__contains: role name contains param
    :query name__startswith: role name starts with param
    :query name__endswith: role name ends with param
    """
    fields = req.get_param_as_list("fields", transform=columns.__getitem__)
    cols = ", ".join(fields) if fields else all_columns
    query = "SELECT %s FROM `role`" % cols
    where_params = []
    where_vals = []
    for key in req.params:
        val = req.get_param(key)
        if key in constraints:
            where_params.append(constraints[key])
            where_vals.append(val)

    where_queries = " AND ".join(where_params)
    if where_queries:
        query = "%s WHERE %s" % (query, where_queries)

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor from the connection wrapper
        cursor = connection.cursor(db.DictCursor)
        cursor.execute(query, where_vals)
        data = cursor.fetchall()
        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit cursor.close() and connection.close() are no longer needed.

    resp.text = json_dumps(data)


@debug_only
def on_post(req, resp):
    data = load_json_body(req)
    # Added a check for the required 'name' key
    new_role = data.get("name")
    if new_role is None:
        # Raise a bad request if name is missing
        raise HTTPError("400 Bad Request", "Missing Parameter", "Missing 'name' in request body")

    # Use the 'with' statement for safe connection and transaction management
    # The ContextualRawConnection will handle rollback if an exception occurs
    # within the 'with' block and commit if `connection.commit()` is called.
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("INSERT INTO `role` (`name`) VALUES (%s)", new_role)
            # Commit the transaction explicitly on success
            connection.commit()
        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            if "Duplicate entry" in err_msg:
                err_msg = 'role "%s" already existed' % new_role
            # Re-raise the exception after formatting the error message
            raise HTTPError("422 Unprocessable Entity", "IntegrityError", err_msg) from e
        # The connection and cursor are automatically closed/released by the 'with' statement
        # No need for a finally block to close connection/cursor anymore.

    resp.status = HTTP_201