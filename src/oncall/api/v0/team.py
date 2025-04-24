# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
import time
import traceback
import uuid
from urllib.parse import unquote

from falcon import HTTP_204, HTTPBadRequest, HTTPError, HTTPNotFound
from ujson import dumps as json_dumps

from ... import db, iris
from ...auth import check_team_auth, login_required
from ...constants import SUPPORTED_TIMEZONES, TEAM_DELETED, TEAM_EDITED
from ...utils import create_audit, invalid_char_reg, load_json_body
from .rosters import get_roster_by_team_id

# Assuming get_user_data is refactored to optionally use a provided connection/cursor (via dbinfo)
# or handle its own connection correctly when none is provided.
from .users import get_user_data

# Columns which may be modified
cols = {
    "name",
    "description",
    "slack_channel",
    "slack_channel_notifications",
    "email",
    "scheduling_timezone",
    "iris_plan",
    "iris_enabled",
    "override_phone_number",
    "api_managed_roster",
}

# Helper functions that take a cursor do NOT manage connections


def populate_team_users(cursor, team_dict):
    # Note: get_user_data here is called without dbinfo, meaning it will open
    # and close a new connection for each user lookup. This is functional
    # but can be inefficient for teams with many users.
    cursor.execute(
        """SELECT `user`.`name` FROM `team_user`
                      JOIN `user` ON `team_user`.`user_id`=`user`.`id`
                      WHERE `team_id`=%s""",
        team_dict["id"],
    )
    team_dict["users"] = dict(
        (
            r["name"],
            get_user_data(None, {"name__eq": r["name"]})[0],
        )  # get_user_data called here
        for r in cursor
    )


def populate_team_admins(cursor, team_dict):
    cursor.execute(
        """SELECT `user`.`name` FROM `team_admin`
                      JOIN `user` ON `team_admin`.`user_id`=`user`.`id`
                      WHERE `team_id`=%s""",
        team_dict["id"],
    )
    team_dict["admins"] = [{"name": r["name"]} for r in cursor]


def populate_team_services(cursor, team_dict):
    cursor.execute(
        """SELECT `service`.`name` FROM `team_service`
                      JOIN `service` ON `team_service`.`service_id`=`service`.`id`
                      WHERE `team_id`=%s""",
        team_dict["id"],
    )
    team_dict["services"] = [r["name"] for r in cursor]


def populate_team_rosters(cursor, team_dict):
    # Assuming get_roster_by_team_id uses the provided cursor
    team_dict["rosters"] = get_roster_by_team_id(cursor, team_dict["id"])


populate_map = {
    "users": populate_team_users,
    "admins": populate_team_admins,
    "services": populate_team_services,
    "rosters": populate_team_rosters,
}


def on_get(req, resp, team):
    """
    Get team info by name. By default, only finds active teams. Allows selection of
    fields, including: users, admins, services, descriptions, and rosters. If no ``fields`` is
    specified in the query string, it defaults to all fields.

    ... (docstring remains the same) ...
    """
    team_name = unquote(
        team
    )  # Renamed variable to avoid shadowing function name
    fields = req.get_param_as_list("fields")
    # Use req.get_param_as_bool for active parameter, default True
    active = req.get_param_as_bool("active", default=True)

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the initial query to get basic team info
        cursor.execute(
            """SELECT `id`, `name`, `email`, `slack_channel`, `slack_channel_notifications`,
                             `scheduling_timezone`, `iris_plan`, `iris_enabled`, `override_phone_number`, `api_managed_roster`, `description`
                      FROM `team` WHERE `name`=%s AND `active` = %s""",
            (team_name, active),  # Use team_name variable
        )
        results = cursor.fetchall()

        print(f"{results = }")

        # Check results and raise HTTPNotFound within the with block
        if not results:
            raise HTTPNotFound(
                description=f"Team '{team_name}' not found or not active"
            )
        [team_info] = results  # Unpack the single result

        # Determine fields to populate
        if not fields:
            # default to get all data
            fields_to_populate = populate_map.keys()
        else:
            # Use only requested fields that exist in populate_map
            fields_to_populate = [f for f in fields if f in populate_map]

        # Call populate functions using the cursor from the with block
        for field in fields_to_populate:
            print(f"{field = }")
            populate = populate_map.get(field)
            print(f"{populate = }")
            # Check again if populate exists (defensive)
            if populate:
                try:
                    populate(cursor, team_info)
                except Exception as e:
                    traceback.print_exc()
                    raise

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched and populated team_info dict
    resp.text = json_dumps(team_info)
    print(f"{team_info = }")


@login_required
def on_put(req, resp, team):
    """
    Edit a team's information. Allows edit of: 'name', 'description', 'slack_channel', 'slack_channel_notifications', 'email', 'scheduling_timezone',
    'iris_plan', 'iris_enabled', 'override_phone_number', 'api_managed_roster'

    ... (docstring remains the same) ...
    """
    team_name = unquote(team)  # Renamed variable
    check_team_auth(team_name, req)  # Use team_name variable
    data = load_json_body(req)

    # Basic validation checks before connecting to DB
    data_cols = data.keys()

    if "name" in data:
        new_team_name = data["name"]
        invalid_char = invalid_char_reg.search(new_team_name)
        if invalid_char:
            raise HTTPBadRequest(
                "invalid team name",
                f'team name contains invalid character "{invalid_char.group()}"',
            )
        elif new_team_name == "":
            raise HTTPBadRequest("invalid team name", "empty team name")

    # validate Iris plan if provided and Iris is configured
    # NOTE: The Iris client call here uses UNSAFE string formatting (%s).
    # If iris_plan comes from user input, this is a potential SSRF or injection vulnerability
    # depending on the iris client library and API. This needs to be fixed in the iris client or its usage.
    if "iris_plan" in data and data["iris_plan"] and iris.client is not None:
        iris_plan = data["iris_plan"]
        try:
            plan_resp = iris.client.get(
                iris.client.url
                + "plans?name=%s&active=1"
                % iris_plan  # UNSAFE string formatting
            )
            if plan_resp.status_code != 200 or plan_resp.json() == []:
                raise HTTPBadRequest(
                    "invalid iris escalation plan",
                    f"no iris plan named {iris_plan} exists",  # Use f-string for safety here
                )
        except Exception as e:
            # Catch potential errors during external Iris call
            raise HTTPError(
                "500 Internal Server Error",
                "External Service Error",
                f"Failed to validate Iris plan {iris_plan}: {e}",
            ) from e

    if "iris_enabled" in data:
        if not isinstance(
            data["iris_enabled"], bool
        ):  # Use isinstance for type checking
            raise HTTPBadRequest(
                "invalid payload", "iris_enabled must be boolean"
            )
    if "api_managed_roster" in data:
        if not isinstance(data["api_managed_roster"], bool):  # Use isinstance
            raise HTTPBadRequest(
                "invalid payload", "api_managed_roster must be boolean"
            )
    if "scheduling_timezone" in data:
        if data["scheduling_timezone"] not in SUPPORTED_TIMEZONES:
            raise HTTPBadRequest(
                "invalid payload",
                f"requested scheduling_timezone is not supported. Supported timezones: {list(SUPPORTED_TIMEZONES)}",  # Format list nicely
            )

    # Build SET clause and query parameters for the UPDATE statement
    set_clause_snippets = []
    query_params = []
    for d in data_cols:
        if d in cols:  # Only include valid columns for update
            set_clause_snippets.append(
                f"`{d}`=%s"
            )  # Use f-string for snippet, %s placeholder
            query_params.append(data[d])  # Add the value to parameters list

    # Only proceed with DB update if there's something to update
    if not set_clause_snippets:
        # If no valid columns were provided in the request body, return 204 No Content
        resp.status = HTTP_204
        return

    # Add the team name for the WHERE clause to the parameters list
    query_params.append(team_name)

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            # Construct the final UPDATE query string
            update_query = f"UPDATE `team` SET {', '.join(set_clause_snippets)} WHERE name=%s"

            # Execute the UPDATE query with parameterized values
            cursor.execute(
                update_query, tuple(query_params)
            )  # Convert list to tuple for execute

            # Check if the team was actually found and updated
            if cursor.rowcount == 0:
                # Team not found with the given name, raise 404
                # Raise this within the with block so rollback happens.
                raise HTTPNotFound(
                    description=f"Team '{team_name}' not found for update"
                )
            # If cursor.rowcount > 1, something is fundamentally wrong, but the query targets name=%s
            # which should be unique.

            # Create audit trail entry
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit(
                {"request_body": data}, team_name, TEAM_EDITED, req, cursor
            )

            # Commit the transaction if the update and audit succeed
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            if "Duplicate entry" in err_msg:
                # Format the error message using the attempted new name if available
                attempted_name = data.get("name", team_name)
                err_msg = f"A team named '{attempted_name}' already exists"
            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e
        # Any other exception raised in the try block will also trigger rollback and cleanup.
        # Do not need a finally block to close connection/cursor; the 'with' statement handles it.

    resp.status = HTTP_204  # Changed to 204 No Content which is standard for successful PUT with no response body


@login_required
def on_delete(req, resp, team):
    """
    Soft delete for teams. Does not remove data from the database, but sets the team's active
    param to false. Note that this means deleted teams' names remain in the namespace, so new
    teams cannot be created with the same name a sa deleted team.

    ... (docstring remains the same) ...
    """
    team_name = unquote(team)  # Renamed variable
    new_team_name = str(uuid.uuid4())  # Renamed variable
    deletion_date = time.time()
    check_team_auth(team_name, req)  # Use team_name variable

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        # Soft delete: set team inactive
        # Execute the UPDATE query with parameterized value
        cursor.execute(
            "UPDATE `team` SET `active` = FALSE WHERE `name`=%s", (team_name,)
        )

        # Check if a team was actually found and updated
        deleted_row_count = cursor.rowcount  # Store rowcount from the update
        if deleted_row_count == 0:
            # Team not found with the given name, raise 404 immediately
            # Raising here within the with block ensures rollback and cleanup.
            raise HTTPNotFound(
                description=f"Team '{team_name}' not found for deletion"
            )

        # If team was found and marked inactive, proceed with other operations in the same transaction

        # Delete future events for the team
        cursor.execute(
            "DELETE FROM `event` WHERE `team_id` = (SELECT `id` FROM `team` WHERE `name` = %s) "
            "AND `start` > UNIX_TIMESTAMP()",
            (team_name,),  # Parameterize the team name in the subquery
        )

        # Create audit trail entry
        # Assuming create_audit takes a cursor and handles DB ops within it
        create_audit({}, team_name, TEAM_DELETED, req, cursor)

        # Get the team ID (needed for the deleted_team insert)
        # This SELECT should happen *after* the initial UPDATE check,
        # but before changing the name in the team table.
        cursor.execute("SELECT `id` FROM `team` WHERE `name`=%s", (team_name,))
        team_id_result = cursor.fetchone()
        if (
            not team_id_result
        ):  # Should not happen if deleted_row_count > 0, but defensive check
            raise HTTPError(
                "500 Internal Server Error",
                "Database Error",
                f"Could not retrieve ID for deleted team '{team_name}'",
            )
        team_id = team_id_result[0]

        # Change name in team table to preserve a clean namespace
        cursor.execute(
            "UPDATE `team` SET `name` = %s WHERE `name`= %s",
            (new_team_name, team_name),
        )

        # Create entry in deleted_teams table
        cursor.execute(
            "INSERT INTO `deleted_team` (team_id, new_name, old_name, deletion_date) VALUES (%s, %s, %s, %s)",
            (team_id, new_team_name, team_name, deletion_date),
        )

        # Commit the entire transaction if all steps succeed
        connection.commit()

        # Do not need to close connection/cursor; the 'with' statement handles it.
        # Any exception raised before commit will trigger rollback and cleanup.

    resp.status = HTTP_204  # Changed to 204 No Content which is standard for successful DELETE
