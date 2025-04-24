# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from collections import defaultdict

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import auth, db
from ...utils import load_json_body

JOIN_CONTACT_TABLEs = (
    " LEFT JOIN `user_contact` ON `user`.`id` = `user_contact`.`user_id`"
    " LEFT JOIN `contact_mode` ON `user_contact`.`mode_id` = `contact_mode`.`id`"
)

columns = {
    "id": "`user`.`id` as `id`",
    "name": "`user`.`name` as `name`",
    "full_name": "`user`.`full_name` as `full_name`",
    "time_zone": "`user`.`time_zone` as `time_zone`",
    "photo_url": "`user`.`photo_url` as `photo_url`",
    # Note: contacts are handled specially in get_user_data processing,
    # the columns specified here are for fetching the raw data rows.
    "contacts": (
        "`contact_mode`.`name` AS `mode`, "
        "`user_contact`.`destination` AS `destination`, "
        "`user`.`id` AS `contact_id`"  # Added contact_id here for easy lookup
    ),
    "active": "`user`.`active` as `active`",
    "god": "`user`.`god` as `god`",
}

# Need all individual column names for the default SELECT list when no fields are specified
# Excluding 'contacts' as it's a pseudo-column handled by joins
all_select_columns = [columns[c] for c in columns if c != "contacts"]
all_select_columns.extend(
    [
        "`contact_mode`.`name` AS `mode`",
        "`user_contact`.`destination` AS `destination`",
        "`user`.`id` AS `contact_id`",
    ]
)
all_columns_clause = ", ".join(all_select_columns)


constraints = {
    "id": "`user`.`id` = %s",
    "id__eq": "`user`.`id` = %s",
    "id__ne": "`user`.`id` != %s",
    "id__lt": "`user`.`id` < %s",
    "id__le": "`user`.`id` <= %s",
    "id__gt": "`user`.`id` > %s",
    "id__ge": "`user`.`id` >= %s",
    "name": "`user`.`name` = %s",
    "name__eq": "`user`.`name` = %s",
    "name__contains": '`user`.`name` LIKE CONCAT("%%", %s, "%%")',
    "name__startswith": '`user`.`name` LIKE CONCAT(%s, "%%")',
    "name__endswith": '`user`.`name` LIKE CONCAT("%%", %s)',
    "full_name": "`user`.`full_name` = %s",
    "full_name__eq": "`user`.`full_name` = %s",
    "full_name__contains": '`user`.`full_name` LIKE CONCAT("%%", %s, "%%")',
    "full_name__startswith": '`user`.`full_name` LIKE CONCAT(%s, "%%")',
    "full_name__endswith": '`user`.`full_name` LIKE CONCAT("%%", %s)',
    "active": "`user`.`active` = %s",
    "god": "`user`.`god` = %s",  # Added god constraint based on columns list
}


def get_user_data(fields, filter_params, dbinfo=None):
    """
    Get user data for a request. Uses parameterized queries for safety.
    Can optionally use an existing connection/cursor from dbinfo.
    """
    contacts_requested = False
    from_clause = "`user`"
    select_cols = []

    if fields:
        # Validate fields and build SELECT clause
        for f in fields:
            if f not in columns:
                raise HTTPBadRequest(
                    "Bad fields", f"Invalid field requested: {f}"
                )
            if f == "contacts":
                contacts_requested = True
            else:
                select_cols.append(columns[f])
    else:
        # Default to all columns including contacts
        contacts_requested = True
        select_cols = [
            columns[c] for c in columns if c != "contacts"
        ]  # Add basic user columns

    # If contacts are requested or if fetching all columns (which includes contact join),
    # ensure the joins are in the FROM clause and add contact-specific select columns.
    if contacts_requested:
        from_clause += JOIN_CONTACT_TABLEs
        # Add contact-specific columns for selection. These are needed regardless
        # of whether 'contacts' was in the fields list, as long as the join happens.
        # We need user.id too for grouping contacts later.
        contact_selects = [
            "`contact_mode`.`name` AS `mode`",
            "`user_contact`.`destination` AS `destination`",
            "`user`.`id` AS `contact_id`",
        ]
        # Prevent duplicates if user.id was already requested
        select_cols.extend(
            [col for col in contact_selects if col not in select_cols]
        )

    # *** SECURITY FIX: Use parameterized queries for the WHERE clause ***
    where_params_snippets = []  # e.g., "`user`.`name` = %s"
    where_values = []  # e.g., ["jdoe"]

    for key, value in filter_params.items():
        if key in constraints:
            where_params_snippets.append(constraints[key])
            where_values.append(
                value
            )  # Append value directly, no escape needed here
        # else: Ignore unknown filter parameters

    where_clause = (
        " AND ".join(where_params_snippets) if where_params_snippets else "1"
    )  # Use "1" for no WHERE conditions

    # Construct the full query string
    # Ensure distinct users when joining contacts if only user fields requested
    # However, we always fetch contact info if joins are present for later processing.
    # A simple SELECT DISTINCT user.id, ... might be better, or just rely on post-processing.
    # The current post-processing handles multiple rows per user due to joins.
    # Let's build the query template using the collected select columns and where clause.
    query = f"SELECT {', '.join(select_cols)} FROM {from_clause}"
    if where_clause != "1":
        query += f" WHERE {where_clause}"

    # *** Connection Management using 'with' or provided dbinfo ***
    data = []  # Initialize data outside the conditional block
    # Use a flag to know if *this* function opened the connection
    connection_opened_here = False

    if dbinfo is None:
        # This function needs to open and manage the connection
        connection_opened_here = True
        try:
            with db.connect() as connection:
                cursor = connection.cursor(db.DictCursor)
                # *** EXECUTE with parameters ***
                print("--- Executing SQL Query ---")
                print(query)
                print("--- With Parameters ---")
                print(where_values)
                print("-------------------------")
                cursor.execute(
                    query, where_values
                )  # Pass values as the second argument
                data = cursor.fetchall()
            # Connection and cursor are automatically closed by the 'with' block
        except Exception as e:
            # Log or handle exceptions during DB interaction
            print(
                f"Error in get_user_data (connection opened here): {e}"
            )  # Replace with proper logging
            raise  # Re-raise the exception for the caller (on_get) to handle
    else:
        # Use the provided connection and cursor
        connection, cursor = dbinfo
        try:
            # *** EXECUTE with parameters ***
            cursor.execute(
                query, where_values
            )  # Pass values as the second argument
            data = cursor.fetchall()
            # Do NOT close connection/cursor here, they are managed by the caller (dbinfo provider)
        except Exception as e:
            # Log or handle exceptions during DB interaction
            print(
                f"Error in get_user_data (using provided connection): {e}"
            )  # Replace with proper logging
            # The caller's context manager will handle rollback/cleanup
            raise  # Re-raise the exception

    # Format contact info (This part remains largely the same, but operates on 'data')
    if contacts_requested:
        # end result accumulator
        ret = {}
        for row in data:
            print(f"{row = }")
            user_id = row.get(
                "contact_id"
            )  # Use .get for safety if column name changes
            # ensure contact_id is present, skip rows if not (shouldn't happen with correct join/select)
            if user_id is None:
                print(
                    f"Warning: Row missing contact_id: {row}"
                )  # Log unexpected data
                continue

            # add data row into accumulator only if not already there
            if user_id not in ret:
                # Copy necessary fields, excluding raw contact details
                user_row = {
                    k: v
                    for k, v in row.items()
                    if k not in ["mode", "destination", "contact_id"]
                }
                ret[user_id] = user_row
                ret[user_id]["contacts"] = {}

            mode = row.get("mode")
            dest = row.get("destination")
            # Add contact info if mode and destination are not None (handles LEFT JOIN results)
            if mode is not None and dest is not None:
                ret[user_id]["contacts"][mode] = dest

        data = list(ret.values())

    return data


def on_get(req, resp):
    """
    Get users filtered by params. Returns a list of user info objects for all users matching
    filter parameters.

    ... (docstring remains the same) ...
    """
    # on_get calls get_user_data, which now handles its own connection management
    # or uses a provided one (not the case here).
    resp.text = json_dumps(
        get_user_data(req.get_param_as_list("fields"), req.params)
    )


@auth.debug_only
def on_post(req, resp):
    """
    Create user. Currently used only in debug mode.
    """
    data = load_json_body(req)
    # Added a check for the required 'name' key
    new_user_name = data.get("name")
    if new_user_name is None:
        # Raise a bad request if name is missing
        raise HTTPError(
            "400 Bad Request",
            "Missing Parameter",
            "Missing 'name' in request body",
        )

    # Use the 'with' statement for safe connection and transaction management
    # The ContextualRawConnection will handle rollback if an exception occurs
    # within the 'with' block and commit if `connection.commit()` is called.
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            # Use the specific parameter name defined in the query (%(name)s)
            cursor.execute(
                "INSERT INTO `user` (`name`) VALUES (%(name)s)",
                {"name": new_user_name},
            )
            # Commit the transaction explicitly on success
            connection.commit()
        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            if "Duplicate entry" in err_msg:
                # Format the error message using the actual name attempted
                err_msg = f'user name "{new_user_name}" already exists'
            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e
        # The connection and cursor are automatically closed/released by the 'with' statement
        # No need for a finally block to close connection/cursor anymore.

    resp.status = HTTP_201
