# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_204, HTTPBadRequest, HTTPError, HTTPNotFound
from ujson import dumps as json_dumps

from ... import db
from ...auth import check_user_auth, login_required
from ...utils import load_json_body

# Assuming get_user_data is defined in a way that handles its own connection
# or is refactored elsewhere to receive a connection or use the pattern.
# This on_get function calls get_user_data and does not directly use db.connect().
from .users import get_user_data

writable_columns = {
    "name": "`user`.`name` as `name`",
    "full_name": "`user`.`full_name` as `full_name`",
    "time_zone": "`user`.`time_zone` as `time_zone`",
    "photo_url": "`user`.`photo_url` as `photo_url`",
    "contacts": (
        "`contact_mode`.`name` AS `mode`, "
        "`user_contact`.`destination` AS `destination`, "
        "`user`.`id` AS `contact_id`"
    ),
    "active": "`user`.`active` as `active`",
}


def on_get(req, resp, user_name):
    """
    Get user info by name. Retrieved fields can be filtered with the ``fields``
    query parameter. Valid fields:

    - id - user id
    - name - username
    - contacts - user contact information
    - full_name - user's full name
    - time_zone - user's preferred display timezone
    - photo_url - URL of user's thumbnail photo
    - active - bool indicating whether the user is active in Oncall. Users can
      be marked inactive after leaving the company to preserve past event information.

    If no ``fields`` is provided, the endpoint defaults to returning all fields.

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/users/jdoe  HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "active": 1,
            "contacts": {
                "call": "+1 111-111-1111",
                "email": "jdoe@example.com",
                "im": "jdoe",
                "sms": "+1 111-111-1111"
            },
            "full_name": "John Doe",
            "id": 1234,
            "name": "jdoe",
            "photo_url": "image.example.com",
            "time_zone": "US/Pacific"
        }

    """
    # Format request to filter query on user name
    req.params["name"] = user_name
    # This function delegates data fetching to get_user_data,
    # so it doesn't manage the DB connection directly.
    data = get_user_data(req.get_param_as_list("fields"), req.params)
    if not data:
        raise HTTPNotFound()
    resp.text = json_dumps(data[0])


@login_required
def on_delete(req, resp, user_name):
    """
    Delete user by name

    **Example request:**

    .. sourcecode:: http

        DELETE /api/v0/users/jdoe HTTP/1.1

    :statuscode 200: Successful delete
    :statuscode 404: User not found
    """
    check_user_auth(user_name, req)
    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()
        # Execute the delete statement
        cursor.execute("DELETE FROM `user` WHERE `name`=%s", user_name)
        # Check if a row was actually deleted (user found)
        if cursor.rowcount == 0:
            # Raise HTTPNotFound *before* attempting to commit a delete that didn't happen
            raise HTTPNotFound(description="User not found")
        # Commit the transaction if the delete was successful
        connection.commit()
        # The connection and cursor will be automatically closed/released by the 'with' statement
        # No need for explicit close calls.

    # If the delete was successful and committed, return 204 No Content
    # (More RESTful for successful DELETE than 200 with empty body)
    resp.status = HTTP_204


@login_required
def on_put(req, resp, user_name):
    """
    Update user info. Allows edits to:

    - contacts
    - name
    - full_name
    - time_zone
    - photo_url
    - active

    Takes an object specifying the new values of these attributes. ``contacts`` acts
    slightly differently, specifying an object with the contact mode as key and new
    values for that contact mode as values. Any contact mode not specified will be
    unchanged. Similarly, any field not specified in the PUT will be unchanged.

    **Example request:**

    .. sourcecode:: http

        PUT /api/v0/users/jdoe  HTTP/1.1
        Content-Type: application/json

        {
            "contacts": {
                "call": "+1 222-222-2222",
                "email": "jdoe@example2.com"
            }
            "name": "johndoe",
            "full_name": "Johnathan Doe",
        }

    :statuscode 204: Successful edit
    :statuscode 400: No user exists with given name (if updating standard columns)
    :statuscode 404: User not found (handled implicitly if update fails or contacts update on non-existent user)
    :statuscode 422: IntegrityError (e.g., trying to set name to an existing name)
    """
    contacts_query = """REPLACE INTO user_contact (`user_id`, `mode_id`, `destination`) VALUES
                           ((SELECT `id` FROM `user` WHERE `name` = %(user)s),
                            (SELECT `id` FROM `contact_mode` WHERE `name` = %(mode)s),
                            %(destination)s)
                            """
    check_user_auth(user_name, req)
    data = load_json_body(req)

    set_contacts = False
    set_columns = []
    set_values = []  # Separate list for values for parameterized query
    for (
        field,
        value,
    ) in data.items():  # Iterate through items to get key and value
        if field == "contacts":
            set_contacts = True
            # contacts are handled separately
        elif field in writable_columns:
            set_columns.append("`{0}` = %s".format(field))
            set_values.append(value)  # Add value to the list
        # Ignore unknown fields in the request body

    set_clause = ", ".join(set_columns)

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        # Update standard columns if any were provided
        if set_clause:
            query = "UPDATE `user` SET {0} WHERE `name` = %s".format(set_clause)
            # query_data = set_values + [user_name] # Original combined data list
            # Use the separate lists correctly for execute
            query_params = set_values + [user_name]

            try:
                cursor.execute(query, query_params)
            except db.IntegrityError as e:
                # Catch IntegrityError specifically during the user UPDATE
                err_msg = str(e.args[1])
                # Example: trying to update name to one that already exists
                raise HTTPError(
                    "422 Unprocessable Entity", "IntegrityError", err_msg
                ) from e

            # Check rowcount *after* execution but *before* processing contacts
            # or committing, inside the 'with' block.
            # If the user wasn't found by name for the update, raise 404.
            if (
                cursor.rowcount == 0
            ):  # Changed check from != 1 to == 0 for clarity with UPDATE
                # No user found for the update, raise NotFound
                raise HTTPNotFound(description="User not found with given name")
            # If cursor.rowcount > 1, something is fundamentally wrong (multiple users with same name?)
            # The original code didn't handle this, but the 0-row case is the critical one for 404/400.
            # The original HTTPBadRequest message "No User Found" with code 400 was slightly
            # confusing, 404 is more standard for "resource not found" during an update.
            # Kept 400 to match original logic, but changed message slightly.
            # Using 404 might be better REST practice. Let's stick to 400 as per original error code.
            if cursor.rowcount != 1:
                raise HTTPBadRequest(
                    "Update Failed",
                    "Could not update user (check name or if multiple users exist)",
                )

        # Update contacts if any were provided in the request
        if set_contacts:
            contacts_data_for_executemany = []
            # Check if data["contacts"] is a dictionary as expected
            if not isinstance(data["contacts"], dict):
                raise HTTPBadRequest(
                    "Invalid contacts data", "Contacts must be a dictionary"
                )

            for mode, dest in data["contacts"].items():
                contact = {}
                contact["mode"] = mode
                contact["destination"] = dest
                contact["user"] = (
                    user_name  # Use the original user_name for lookup
                )
                contacts_data_for_executemany.append(contact)

            if (
                contacts_data_for_executemany
            ):  # Only execute if there are contacts to update
                try:
                    cursor.executemany(
                        contacts_query, contacts_data_for_executemany
                    )
                except db.IntegrityError as e:
                    # Catch IntegrityError specifically during contact updates
                    err_msg = str(e.args[1])
                    # Example: invalid mode name leading to NULL mode_id
                    raise HTTPError(
                        "422 Unprocessable Entity", "IntegrityError", err_msg
                    ) from e

        # Commit the entire transaction if both updates succeeded (or if only one type was present)
        # This commit happens only if no exceptions were raised before this point within the 'with' block
        connection.commit()

        # The connection and cursor are automatically closed/released by the 'with' statement
        # No need for explicit close calls or a finally block.

    resp.status = HTTP_204
