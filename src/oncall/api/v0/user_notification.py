# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTP_201  # Added HTTP_201 implicitly used by PUT success
from falcon import HTTPBadRequest, HTTPNotFound

from ... import db
from ...auth import check_user_auth, login_required
from ...utils import load_json_body

# Used in on_put
columns = {
    "team": "`team_id` = (SELECT `id` FROM `team` WHERE `name` = %s)",
    "mode": "`mode_id` = (SELECT `id` FROM `contact_mode` WHERE `name` = %s)",
    "type": "`type_id` = (SELECT `id` FROM `notification_type` WHERE `name` = %s)",
    "time_before": "`time_before` = %s",
    "only_if_involved": "`only_if_involved` = %s",
}


@login_required
def on_delete(req, resp, notification_id):
    """
    Delete user notification settings by id.

    **Example request:**

    .. sourcecode:: http

        DELETE /api/v0/notifications/1234   HTTP/1.1

    :statuscode 200: Successful delete
    :statuscode 404: Notification setting not found
    """
    num_deleted = 0
    # Use 'with' block for database operations and transaction management
    with db.connect() as connection:
        # Using standard cursor as original code did
        cursor = connection.cursor()
        try:
            # Check ownership first
            cursor.execute(
                """SELECT `user`.`name`
                   FROM `notification_setting`
                   JOIN `user` ON `notification_setting`.`user_id` = `user`.`id`
                   WHERE `notification_setting`.`id` = %s""",
                (notification_id,),  # Pass as tuple
            )
            result = cursor.fetchone()
            if not result:
                # No setting found with this ID
                raise HTTPNotFound(
                    description=f"Notification setting ID '{notification_id}' not found."
                )

            username = result[0]
            check_user_auth(username, req)  # Check authorization

            # Proceed with deletion
            cursor.execute(
                "DELETE FROM notification_setting WHERE `id` = %s",
                (notification_id,),  # Pass as tuple
            )
            num_deleted = cursor.rowcount

            # If we reach here, operations were successful
            connection.commit()

        except HTTPNotFound:
            raise  # Re-raise HTTPNotFound immediately
        except Exception as e:
            connection.rollback()  # Rollback on any other error
            # Log the error e
            # Re-raise or raise a generic server error if appropriate
            raise HTTPBadRequest(
                "Deletion Failed", f"An error occurred: {e}"
            ) from e
        # Connection and cursor are automatically closed by 'with' block

    # Check if deletion actually happened (might be redundant if commit succeeded)
    # This handles edge cases where the row might disappear between SELECT and DELETE
    # Or if the ID was found but delete affected 0 rows for some reason.
    if num_deleted == 0:
        # If commit succeeded but num_deleted is 0, it implies the record wasn't there
        # during the DELETE operation, which is effectively a NotFound scenario.
        # However, the initial check should catch this. If we get here, it's unusual.
        # For safety, let's treat it as NotFound as the original code did (!= 1 check)
        raise HTTPNotFound(
            description=f"Notification setting ID '{notification_id}' could not be deleted (may already be gone)."
        )
    # If num_deleted == 1 and commit succeeded, the response will be 200 OK by default.


@login_required
def on_put(req, resp, notification_id):
    """
    Edit user notification settings. Allows editing of the following attributes:
    ... (docstring content unchanged) ...
    """
    data = load_json_body(req)
    params = list(data.keys())  # Use list for ordered iteration if needed later

    # Extract roles if present, handle potential KeyError if not required by PUT
    roles = data.pop("roles", None)  # Use pop with default None

    # Prepare columns and parameters for the UPDATE statement
    cols_to_update = [columns[c] for c in params if c in columns]
    query_params = [data[c] for c in params if c in columns]

    # Construct dynamic part of the UPDATE query only if there are columns to update
    update_query_part = ""
    if cols_to_update:
        update_query_part = (
            "UPDATE notification_setting SET %s WHERE id = %%s"
            % ", ".join(cols_to_update)
        )

    # Use 'with' block for all database operations
    with db.connect() as connection:
        # Ensure DictCursor is available as original code used it
        if not db.DictCursor:
            raise RuntimeError(
                "DictCursor is required but not available. Check DBAPI driver and db.init()."
            )
        cursor = connection.cursor(db.DictCursor)

        try:
            # 1. Fetch current setting and check ownership
            cursor.execute(
                """SELECT ns.`time_before`, ns.`only_if_involved`,
                           nt.`is_reminder`, u.`name` as `username`
                    FROM `notification_setting` ns
                    JOIN `notification_type` nt ON ns.`type_id` = nt.`id`
                    JOIN `user` u ON ns.`user_id` = u.`id`
                    WHERE ns.`id` = %s""",
                (notification_id,),
            )
            current_setting = cursor.fetchone()
            if not current_setting:
                raise HTTPNotFound(
                    description=f"Notification setting ID '{notification_id}' not found."
                )

            # Check authorization
            check_user_auth(current_setting["username"], req)

            # 2. Determine final reminder status and validate params
            is_reminder = current_setting["is_reminder"]
            notification_type = data.get(
                "type"
            )  # Check if type is being updated

            if notification_type:
                # If type is updated, fetch its reminder status
                cursor.execute(
                    "SELECT is_reminder FROM notification_type WHERE name = %s",
                    (notification_type,),
                )
                new_type_info = cursor.fetchone()
                if not new_type_info:
                    raise HTTPBadRequest(
                        "Invalid Parameter",
                        f"Notification type '{notification_type}' not found.",
                    )
                is_reminder = new_type_info["is_reminder"]

            # Determine final values for time_before/only_if_involved
            # Use get(key, default) where default is the current value
            time_before = data.get(
                "time_before", current_setting["time_before"]
            )
            only_if_involved = data.get(
                "only_if_involved", current_setting["only_if_involved"]
            )

            # Perform validation checks based on final reminder status
            if is_reminder:
                if (
                    "only_if_involved" in data
                    and data["only_if_involved"] is not None
                ):
                    # Explicitly trying to set only_if_involved on a reminder
                    raise HTTPBadRequest(
                        "invalid setting update",
                        "reminder setting cannot define 'only_if_involved'",
                    )
                if (
                    "time_before" not in data
                    and current_setting["time_before"] is None
                ):
                    # Implicitly leaving time_before unset on a reminder (should have a value)
                    raise HTTPBadRequest(
                        "invalid setting update",
                        "reminder setting must define 'time_before'",
                    )
                # Ensure only_if_involved is NULL if it's a reminder (even if not in request data)
                if (
                    "only_if_involved" in columns
                    and "only_if_involved" not in data
                ):
                    cols_to_update.append("`only_if_involved` = NULL")
                    # No corresponding parameter needed for NULL literal
            else:  # Not a reminder
                if "time_before" in data and data["time_before"] is not None:
                    # Explicitly trying to set time_before on a non-reminder
                    raise HTTPBadRequest(
                        "invalid setting update",
                        "non-reminder setting cannot define 'time_before'",
                    )
                if (
                    "only_if_involved" not in data
                    and current_setting["only_if_involved"] is None
                ):
                    # Implicitly leaving only_if_involved unset on a non-reminder (should have a value)
                    raise HTTPBadRequest(
                        "invalid setting update",
                        "non-reminder setting must define 'only_if_involved'",
                    )
                # Ensure time_before is NULL if it's not a reminder (even if not in request data)
                if "time_before" in columns and "time_before" not in data:
                    cols_to_update.append("`time_before` = NULL")
                    # No corresponding parameter needed for NULL literal

            # 3. Perform UPDATE if needed
            if cols_to_update:
                # Reconstruct query and params if NULL columns were added
                final_update_query = (
                    "UPDATE notification_setting SET %s WHERE id = %%s"
                    % ", ".join(cols_to_update)
                )
                final_query_params = tuple(query_params + [notification_id])
                cursor.execute(final_update_query, final_query_params)
                if cursor.rowcount == 0:
                    # Should not happen if initial check passed, but indicates concurrent delete maybe
                    raise HTTPNotFound(
                        description=f"Notification setting ID '{notification_id}' potentially deleted during update."
                    )

            # 4. Update roles if provided
            if (
                roles is not None
            ):  # Check if roles key was actually present in request
                if not isinstance(roles, list):  # Ensure it's a list
                    raise HTTPBadRequest(
                        "Invalid Parameter", "'roles' must be a list."
                    )

                # Delete existing roles for this setting
                cursor.execute(
                    "DELETE FROM `setting_role` WHERE `setting_id` = %s",
                    (notification_id,),
                )

                # Insert new roles if the list is not empty
                if roles:
                    role_insert_query_part = ", ".join(
                        ["(%s, (SELECT `id` FROM `role` WHERE `name` = %s))"]
                        * len(roles)
                    )
                    # Prepare parameters: flatten list with setting_id prepended to each role
                    role_params = []
                    for role_name in roles:
                        role_params.extend([notification_id, role_name])

                    cursor.execute(
                        "INSERT INTO `setting_role`(`setting_id`, `role_id`) VALUES "
                        + role_insert_query_part,
                        tuple(role_params),
                    )

            # 5. Commit transaction if all steps succeeded
            connection.commit()

        except (HTTPNotFound, HTTPBadRequest) as e:
            connection.rollback()  # Rollback on known client errors too before re-raising
            raise e
        except (db.Error, db.IntegrityError) as e:  # Catch specific DB errors
            connection.rollback()
            # Check for common issues like invalid role name during INSERT
            error_msg = f"Database error during update: {e}"
            if (
                "foreign key constraint" in str(e).lower()
                or "cannot find role" in str(e).lower()
            ):  # Adapt based on actual errors
                error_msg = "Invalid reference provided (e.g., team, mode, type, role not found)."
            raise HTTPBadRequest("Update Failed", error_msg) from e
        except Exception as e:  # Catch other unexpected errors
            connection.rollback()
            # Log error e
            raise  # Re-raise the original exception

        # Connection and cursor automatically closed

    # Default response is 200 OK if no exception was raised
