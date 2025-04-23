# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
import time

# uuid imported in previous file, but not here? Added if needed.
# from uuid import uuid4

# operator, defaultdict imported in previous file, but not here? Added if needed.
# import operator
# from collections import defaultdict

from urllib.parse import unquote

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import constants, db
from ...auth import check_calendar_auth, login_required
from ...constants import EVENT_CREATED
from ...utils import (
    create_audit, # Assuming create_audit takes a cursor
    create_notification, # Assuming create_notification takes a cursor
    load_json_body,
    user_in_team_by_name, # Assuming user_in_team_by_name takes a cursor
)

columns = {
    "id": "`event`.`id` as `id`",
    "start": "`event`.`start` as `start`",
    "end": "`event`.`end` as `end`",
    "role": "`role`.`name` as `role`",
    "team": "`team`.`name` as `team`",
    "user": "`user`.`name` as `user`",
    "full_name": "`user`.`full_name` as `full_name`",
    "schedule_id": "`event`.`schedule_id`",
    "link_id": "`event`.`link_id`",
    "note": "`event`.`note`",
}

all_columns_select_clause = ", ".join(columns.values())

constraints = {
    "id": "`event`.`id` = %s",
    "id__eq": "`event`.`id` = %s",
    "id__ne": "`event`.`id` != %s",
    "id__gt": "`event`.`id` > %s",
    "id__ge": "`event`.`id` >= %s",
    "id__lt": "`event`.`id` < %s",
    "id__le": "`event`.`id` <= %s",
    "start": "`event`.`start` = %s",
    "start__eq": "`event`.`start` = %s",
    "start__ne": "`event`.`start` != %s",
    "start__gt": "`event`.`start` > %s",
    "start__ge": "`event`.`start` >= %s",
    "start__lt": "`event`.`start` < %s",
    "start__le": "`event`.`start` <= %s",
    "end": "`event`.`end` = %s",
    "end__eq": "`event`.`end` = %s",
    "end__ne": "`event`.`end` != %s",
    "end__gt": "`event`.`end` > %s",
    "end__ge": "`event`.`end` >= %s",
    "end__lt": "`event`.`end` < %s",
    "end__le": "`event`.`end` <= %s",
    "role": "`role`.`name` = %s", # Constraint on role name
    "role__eq": "`role`.`name` = %s",
    "role__contains": '`role`.`name` LIKE CONCAT("%%", %s, "%%")',
    "role__startswith": '`role`.`name` LIKE CONCAT(%s, "%%")',
    "role__endswith": '`role`.`name` LIKE CONCAT("%%", %s)',
    "team": "`team`.`name` = %s", # Constraint on team name
    "team__eq": "`team`.`name` = %s",
    "team__contains": '`team`.`name` LIKE CONCAT("%%", %s, "%%")',
    "team__startswith": '`team`.`name` LIKE CONCAT(%s, "%%")',
    "team__endswith": '`team`.`name` LIKE CONCAT("%%", %s)',
    "team_id": "`team`.`id` = %s", # Constraint on team ID
    "user": "`user`.`name` = %s", # Constraint on user name
    "user__eq": "`user`.`name` = %s",
    "user__contains": '`user`.`name` LIKE CONCAT("%%", %s, "%%")',
    "user__startswith": '`user`.`name` LIKE CONCAT(%s, "%%")',
    "user__endswith": '`user`.`name` LIKE CONCAT("%%", %s)',
    # Assuming other potential constraints are handled by the logic below
}

TEAM_CONSTRAINT_KEYS = {
    "team",
    "team__eq",
    "team__contains",
    "team__startswith",
    "team__endswith", # Fixed typo 'team_endswith' to 'team__endswith'
    "team_id",
}


def on_get(req, resp):
    """
    Search for events. Allows filtering based on a number of parameters,
    detailed below.

    ... (docstring remains the same) ...
    """
    fields = req.get_param_as_list("fields")
    select_cols = []
    if fields:
        # Validate fields and build SELECT clause
        for f in fields:
            if f not in columns:
                 raise HTTPBadRequest("Bad fields", f"Invalid field requested: {f}")
            select_cols.append(columns[f])
    else:
        select_cols = list(columns.values()) # Default to all columns

    cols_clause = ", ".join(select_cols)

    # Base query with necessary joins
    query_template = (
        f"""SELECT {cols_clause} FROM `event`
               JOIN `user` ON `user`.`id` = `event`.`user_id`
               JOIN `team` ON `team`.`id` = `event`.`team_id`
               JOIN `role` ON `role`.`id` = `event`.`role_id`"""
    )

    # Get include_subscribed parameter, default to True
    include_sub = req.get_param_as_bool("include_subscribed", default=True)


    # *** SECURITY FIX: Use parameterized queries to build complex WHERE clause ***
    conditions = [] # List of WHERE clause snippets (e.g., "`field` = %s")
    values = []     # List of corresponding values (e.g., ["value"])

    # 1. Process non-team constraint parameters
    non_team_params = req.params.keys() - TEAM_CONSTRAINT_KEYS
    for key in non_team_params:
        val = req.get_param(key)
        # Check if key is a valid constraint *and* not empty/None if applicable
        if key in constraints and val is not None: # Added None check
            # Special handling for list parameters like 'action' if added later
            # Assuming all current constraints map directly to a single value %s
            conditions.append(constraints[key]) # Add snippet with placeholder
            values.append(val) # Add the value

    # 2. Process team constraint parameters and handle subscriptions if requested
    team_conditions_snippets = []
    team_values = []
    team_params_from_req = req.params.keys() & TEAM_CONSTRAINT_KEYS # Use '&' for set intersection

    if team_params_from_req:
        for key in team_params_from_req:
            val = req.get_param(key)
            if val is not None: # Added None check
                 team_conditions_snippets.append(constraints[key]) # Add snippet with placeholder
                 team_values.append(val) # Add the value

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        cursor = connection.cursor(db.DictCursor)

        # Handle team subscriptions if including them and team parameters were provided
        if include_sub and team_conditions_snippets:
            # Build condition string for the subscription query WHERE clause
            team_subs_where_clause = " AND ".join(team_conditions_snippets)

            # Execute query to get subscriptions based on team parameters using parameters
            subs_query = f"""SELECT `subscription_id`, `role_id` FROM `team_subscription`
                             JOIN `team` ON `team_id` = `team`.`id`
                             WHERE {team_subs_where_clause}"""

            # Execute subs_query with team_values.
            # This query uses team_values as parameters.
            cursor.execute(subs_query, team_values)
            subs_results = cursor.fetchall()

            # Build the OR group for the main event query WHERE clause
            or_conditions_snippets = []
            or_values = []

            # Add original team constraints (e.g., team.name = 'foo') as part of the OR group
            if team_conditions_snippets:
                or_conditions_snippets.append("(" + " AND ".join(team_conditions_snippets) + ")")
                or_values.extend(team_values) # Add the values for these constraints

            # Add subscription conditions (team_id = sub_id AND role_id = sub_role_id)
            # *** FIX: Add subscription IDs and Role IDs as PARAMETERS, not formatted into the string ***
            for row in subs_results:
                or_conditions_snippets.append("(`team`.`id` = %s AND `role`.`id` = %s)") # Snippet with placeholders
                or_values.extend([row.get("subscription_id"), row.get("role_id")]) # Add values using .get for safety

            # Combine OR conditions into a single group if any were added
            if or_conditions_snippets:
                conditions.append("(" + " OR ".join(or_conditions_snippets) + ")") # Group OR conditions
                values.extend(or_values) # Add ALL values for the OR group
        else: # No subscriptions or no team params, just add team constraints directly (if any)
            if team_conditions_snippets:
                conditions.append("(" + " AND ".join(team_conditions_snippets) + ")")
                values.extend(team_values)


        # Combine all conditions into the final WHERE clause string
        final_where_clause = " AND ".join(conditions) if conditions else "1" # Use "1" for no WHERE conditions

        # Construct the final event query string template
        final_query = f"{query_template} WHERE {final_where_clause}" if final_where_clause != "1" else query_template

        # Add optional ordering (good practice)
        # final_query += " ORDER BY `event`.`start` ASC" # Example ordering


        # *** EXECUTE FINAL QUERY with ALL collected parameters ***
        cursor.execute(final_query, values) # Pass ALL collected values

        # Fetch all results
        data = cursor.fetchall()

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'data' list
    resp.text = json_dumps(data)


@login_required
def on_post(req, resp):
    """
    Endpoint for creating event. Responds with event id for created event. Events must
    specify the following parameters:

    - start: Unix timestamp for the event start time (seconds)
    - end: Unix timestamp for the event end time (seconds)
    - user: Username for the event's user
    - team: Name for the event's team
    - role: Name for the event's role

    All of these parameters are required.

    ... (docstring remains the same) ...
    """
    data = load_json_body(req)
    now = time.time()

    # Basic validation checks before DB interaction
    # Ensure required fields are present
    required_fields = {"start", "end", "user", "team", "role"}
    if not required_fields.issubset(data.keys()):
         missing = required_fields - data.keys()
         raise HTTPBadRequest("Invalid event", f"Missing required parameters: {', '.join(missing)}")

    # Ensure start and end are numbers
    try:
        data["start"] = int(data["start"]) # Attempt conversion if not already int
        data["end"] = int(data["end"])
    except (ValueError, TypeError):
         raise HTTPBadRequest("Invalid event", "start and end timestamps must be integers")


    if data["start"] < now - constants.GRACE_PERIOD:
        raise HTTPBadRequest(
            "Invalid event", "Creating events in the past not allowed"
        )
    if data["start"] >= data["end"]:
        raise HTTPBadRequest("Invalid event", "Event must start before it ends")

    # Ensure team, user, role names are strings
    if not isinstance(data.get("team"), str) or not isinstance(data.get("user"), str) or not isinstance(data.get("role"), str):
        raise HTTPBadRequest("Invalid event", "team, user, and role names must be strings")

    check_calendar_auth(data["team"], req)

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Check if user is in the team using the current cursor
        # Assuming user_in_team_by_name takes a cursor and handles its own query/check
        if not user_in_team_by_name(cursor, data["user"], data["team"]):
            # Raise exception within the with block
            raise HTTPBadRequest("Invalid event", "User must be part of the team")

        # Define columns and values for the INSERT query
        insert_columns = ["`start`", "`end`", "`user_id`", "`team_id`", "`role_id`"]
        # Use dictionary-style placeholders for values, matching keys in the data dict
        insert_values_placeholders = [
            "%(start)s",
            "%(end)s",
            "(SELECT `id` FROM `user` WHERE `name`=%(user)s)",
            "(SELECT `id` FROM `team` WHERE `name`=%(team)s)",
            "(SELECT `id` FROM `role` WHERE `name`=%(role)s)",
        ]

        if "schedule_id" in data:
            # Ensure schedule_id is an integer if present
            schedule_id_val = data["schedule_id"]
            if not isinstance(schedule_id_val, int):
                 try:
                     data["schedule_id"] = int(schedule_id_val)
                 except (ValueError, TypeError):
                     raise HTTPBadRequest("Invalid event", "schedule_id must be an integer")
            insert_columns.append("`schedule_id`")
            insert_values_placeholders.append("%(schedule_id)s") # Placeholder for schedule_id

        if "note" in data:
            # Ensure note is a string if present
            note_val = data["note"]
            if not isinstance(note_val, (str, type(None))): # Allow None if DB supports it
                 raise HTTPBadRequest("Invalid event", "note must be a string or null")
            insert_columns.append("`note`")
            insert_values_placeholders.append("%(note)s") # Placeholder for note

        # Construct the INSERT query string template
        query = "INSERT INTO `event` (%s) VALUES (%s)" % (
            ",".join(insert_columns),
            ",".join(insert_values_placeholders),
        )

        try:
            # Execute the INSERT query with the data dictionary as parameters
            # The DBAPI will map %(key)s placeholders to the keys in the data dictionary.
            cursor.execute(query, data)

            # Get the ID of the newly created event
            event_id = cursor.lastrowid
            if event_id is None:
                 raise HTTPError("500 Internal Server Error", "Database Error", "Failed to retrieve new event ID")

            # Select event info for notifications/audit using the same cursor
            cursor.execute(
                "SELECT team_id, role_id, user_id, start, full_name "
                "FROM event JOIN user ON user.`id` = user_id WHERE event.id=%s",
                (event_id,), # Parameterize event_id as a tuple
            )
            ev_info = cursor.fetchone()
            if not ev_info: # Should not happen if lastrowid worked, but defensive check
                 raise HTTPError("500 Internal Server Error", "Database Error", f"Could not retrieve info for new event ID {event_id}")


            # Prepare context for notification/audit
            context = {
                "team": data["team"], # Use data dict values
                "role": data["role"],
                "full_name": ev_info["full_name"],
            }

            # Create notification using the same cursor
            # Assuming create_notification takes a cursor and handles DB ops within it
            create_notification(
                context,
                ev_info["team_id"],
                [ev_info["role_id"]],
                EVENT_CREATED,
                [ev_info["user_id"]],
                cursor,
                start_time=ev_info["start"],
            )

            # Create audit trail entry using the same cursor
            # Assuming create_audit takes a cursor and handles DB ops within it
            create_audit(
                {"new_event_id": event_id, "request_body": data},
                data["team"],
                EVENT_CREATED,
                req,
                cursor,
            )

            # Commit the transaction if all steps in the try block succeed
            connection.commit()

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback
            # when an exception occurs within the block.
            err_msg = str(e.args[1])
            # Improve error messages based on potential NULLs due to non-existent names
            if "Column 'role_id' cannot be null" in err_msg:
                err_msg = f'role "{data.get("role")}" not found'
            elif "Column 'user_id' cannot be null" in err_msg:
                err_msg = f'user "{data.get("user")}" not found'
            elif "Column 'team_id' cannot be null" in err_msg:
                err_msg = f'team "{data.get("team")}" not found'
            # Add other potential IntegrityError checks if applicable (e.g., foreign key to schedule_id)
            else:
                 # Generic fallback for other integrity errors
                 err_msg = f"Database Integrity Error: {err_msg}"

            # Re-raise the exception after formatting the error message
            raise HTTPError("422 Unprocessable Entity", "IntegrityError", err_msg) from e
        # Any other exception raised in the try block will also trigger rollback and cleanup.
        # The finally block is no longer needed for close calls.

    resp.status = HTTP_201
    resp.text = json_dumps(event_id) # Respond with the created event_id