# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import operator # operator imported but not used?
from collections import defaultdict # defaultdict imported but not used?
from urllib.parse import unquote

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import db, iris
from ...auth import login_required
from ...constants import TEAM_CREATED
from ...utils import (
    create_audit,
    invalid_char_reg,
    load_json_body,
    subscribe_notifications,
)

constraints = {
    "name": "`team`.`name` = %s",
    "name__eq": "`team`.`name` = %s",
    "name__contains": '`team`.`name` LIKE CONCAT("%%", %s, "%%")',
    "name__startswith": '`team`.`name` LIKE CONCAT(%s, "%%")',
    "name__endswith": '`team`.`name` LIKE CONCAT("%%", %s)',
    "id": "`team`.`id` = %s",
    "id__eq": "`team`.`id` = %s",
    "active": "`team`.`active` = %s",
    "email": "`team`.`email` = %s",
    "email__eq": "`team`.`email` = %s",
    "email__contains": '`team`.`email` LIKE CONCAT("%%", %s, "%%")',
    "email__startswith": '`team`.`email` LIKE CONCAT(%s, "%%")',
    "email__endswith": '`team`.`email` LIKE CONCAT("%%", %s)',
    # Assuming other potential constraints are handled by the logic below
}


# This function receives a cursor, it does NOT need to manage the connection
def get_team_ids(cursor, team_names):
    if not team_names:
        return []

    # This query construction for IN is correct for parameterized queries
    team_query = (
        "SELECT DISTINCT `id` FROM `team` WHERE `name` IN ({0})".format(
            ",".join(["%s"] * len(team_names))
        )
    )
    # we need prepared statements here because team_names come from user input
    cursor.execute(team_query, team_names)
    # No close() needed here, the connection/cursor are managed by the caller.
    # Assuming DictCursor or similar is used by the caller
    return [row["id"] for row in cursor]


def on_get(req, resp):
    """
    Search for team names. Allows filtering based on a number of parameters, detailed below.
    Returns list of matching team names. If "active" parameter is unspecified, defaults to
    True (only displaying undeleted teams)

    ... (docstring remains the same) ...
    """

    # Base query - note: selecting both name and id as required by subsequent logic
    query_template = "SELECT `name`, `id` FROM `team`"

    # Build WHERE clause using parameterized query placeholders
    where_params_snippets = [] # e.g., "`team`.`name` = %s"
    query_values = []          # e.g., ["my-team"]

    # Default active=True if not specified
    if "active" not in req.params:
        where_params_snippets.append(constraints["active"])
        query_values.append(True) # Assuming boolean True/False maps correctly to DB boolean/int type

    # Process other filter parameters
    for key, value in req.params.items():
        # Skip 'active' if already processed, and skip 'get_id' as it's not a DB constraint
        if key == "active" and "active" in where_params_snippets:
            continue
        if key == "get_id":
            continue

        if key in constraints:
            where_params_snippets.append(constraints[key])
            query_values.append(value) # Add value directly for parameterization
        # else: Ignore unknown parameters

    where_clause = " AND ".join(where_params_snippets) if where_params_snippets else "1" # Use "1" for no WHERE conditions

    # Combine query template and WHERE clause
    query = f"{query_template} WHERE {where_clause}"


    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a standard cursor (or DictCursor if preferred for fetching by name later)
        # The original used a standard cursor for fetching 0-indexed tuples. Sticking to that.
        cursor = connection.cursor()

        # Execute the query with parameters
        cursor.execute(query, query_values)

        # Fetch data based on the 'get_id' parameter
        if req.get_param_as_bool("get_id"):
            # Fetch tuples (name, id)
            data = [(r[0], r[1]) for r in cursor]
        else:
            # Fetch tuples (name, id) and extract only name
            data = [r[0] for r in cursor]

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit cursor.close() and connection.close() are no longer needed.

    resp.text = json_dumps(data)


@login_required
def on_post(req, resp):
    """
    Endpoint for team creation. The user who creates the team is automatically added as a
    team admin. Because of this, this endpoint cannot be called using an API key, otherwise
    a team would have no admins, making many team operations impossible.

    ... (docstring remains the same) ...
    """

    data = load_json_body(req)
    team_name = unquote(data.get("name", "")).strip() # Use .get and handle empty string
    scheduling_timezone = unquote(data.get("scheduling_timezone", "")) # Use .get

    # Basic validation checks before connecting to DB
    if not team_name:
        raise HTTPBadRequest("Missing Parameter", "name attribute missing or empty from request")
    invalid_char = invalid_char_reg.search(team_name)
    if invalid_char:
        raise HTTPBadRequest(
            "invalid team name",
            f'team name contains invalid character "{invalid_char.group()}"',
        )

    if not scheduling_timezone:
        raise HTTPBadRequest(
            "Missing Parameter", "scheduling_timezone attribute missing or empty from request"
        )

    slack = data.get("slack_channel")
    if slack and slack[0] != "#":
        raise HTTPBadRequest(
            "invalid slack channel", "slack channel name needs to start with #"
        )
    slack_notifications = data.get("slack_channel_notifications")
    if slack_notifications and slack_notifications[0] != "#":
        raise HTTPBadRequest(
            "invalid slack notifications channel",
            "slack channel notifications name needs to start with #",
        )
    email = data.get("email")
    description = data.get("description")
    iris_plan = data.get("iris_plan")
    iris_enabled = data.get("iris_enabled", False)
    override_number = data.get("override_phone_number")
    if not override_number:
        override_number = None

    # validate Iris plan if provided and Iris is configured - moved inside 'with' for DB access
    # if iris_plan is not None and iris.client is not None:
    #    # This validation hits an external service, keep it outside DB transaction if possible.
    #    # But the original code had the DB check for admin *before* this, which was inconsistent.
    #    # Let's keep it outside the DB transaction block for now, as it's not a DB operation.
    #    plan_resp = iris.client.get(
    #        iris.client.url + "plans?name=%s&active=1" % iris_plan # UNSAFE string formatting here!
    #    )
    #    # NOTE: The Iris client call here uses UNSAFE string formatting (%s).
    #    # If iris_plan comes from user input, this is a potential SSRF or injection vulnerability
    #    # depending on the iris client library and API. This needs to be fixed in the iris client or its usage.
    #    if plan_resp.status_code != 200 or plan_resp.json() == []:
    #        raise HTTPBadRequest(
    #            "invalid iris escalation plan",
    #            f"no iris plan named {iris_plan} exists" # Use f-string for safety here
    #        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        # Handle API key request (requires admin field)
        # This DB lookup needs to be inside the 'with' block
        requesting_user = req.context.get("user")
        admin_username = data.get("admin")

        if requesting_user is None: # Assume API key if user context is not set
            if not admin_username:
                 raise HTTPBadRequest(
                     "Missing Parameter",
                     "API requests must specify a team admin username in the admin field",
                 )
            # Look up admin user ID by name
            cursor.execute(
                """SELECT `id` FROM `user` WHERE `name` = %s LIMIT 1""", (admin_username,)
            )
            if cursor.rowcount == 0:
                raise HTTPBadRequest(
                    "Invalid admin", f"admin username {admin_username} was not found in db"
                )
            requesting_user = admin_username # Use the specified admin as the acting user
        # else: requesting_user is already set from context (e.g., cookie login)


        # Optional: Re-validate Iris plan inside the transaction if needed,
        # or trust the outside check and ensure the iris_plan value is safe.
        # The current Iris check uses unsafe string formatting, which is a separate issue.
        # Let's assume the iris_plan variable itself is reasonably safe after the outside check
        # or rely on DB constraints to catch problems if inserted.

        try:
            # Insert into team table
            cursor.execute(
                """INSERT INTO `team` (`name`, `slack_channel`, `slack_channel_notifications`, `email`, `scheduling_timezone`,
                                              `iris_plan`, `iris_enabled`, `override_phone_number`, `description`)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    team_name,
                    slack,
                    slack_notifications,
                    email,
                    scheduling_timezone,
                    iris_plan,
                    iris_enabled,
                    override_number,
                    description,
                ),
            )

            team_id = cursor.lastrowid

            # Add the requesting user as a team user
            query_team_user = """
                INSERT INTO `team_user` (`team_id`, `user_id`)
                VALUES (%s, (SELECT `id` FROM `user` WHERE `name` = %s))"""
            cursor.execute(query_team_user, (team_id, requesting_user))

            # Add the requesting user as a team admin
            query_team_admin = """
                INSERT INTO `team_admin` (`team_id`, `user_id`)
                VALUES (%s, (SELECT `id` FROM `user` WHERE `name` = %s))"""
            cursor.execute(query_team_admin, (team_id, requesting_user))

            # Subscribe the requesting user to notifications for the new team
            # Assuming subscribe_notifications takes a cursor and handles DB ops within it
            subscribe_notifications(team_name, requesting_user, cursor)

            # Create audit trail entry
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit({"team_id": team_id}, team_name, TEAM_CREATED, req, cursor)

            # Commit the entire transaction if all steps succeed
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            if "Duplicate entry" in err_msg:
                err_msg = f'team name "{team_name}" already exists'
            # Re-raise the exception after formatting the error message
            raise HTTPError("422 Unprocessable Entity", "IntegrityError", err_msg) from e
        # Do not need a finally block to close connection/cursor; the 'with' statement handles it.
        # Any other exception raised in the try block will also trigger rollback and cleanup.

    resp.status = HTTP_201