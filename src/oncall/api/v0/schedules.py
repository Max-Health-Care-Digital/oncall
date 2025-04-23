# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.
import operator  # operator imported but not used?
import time  # time is imported but not used?
import uuid  # uuid is imported but not used?
from collections import defaultdict
from urllib.parse import unquote

from falcon import (
    HTTP_201,
    HTTPBadRequest,
    HTTPError,
    HTTPInternalServerError,
    HTTPNotFound,
)
from ujson import dumps as json_dumps

from ... import db, iris  # iris is imported but not used?
from ...auth import check_team_auth, login_required
from ...constants import (
    SUPPORTED_TIMEZONES,
)  # SUPPORTED_TIMEZONES imported but not used?
from ...utils import create_audit  # create_audit imported but not used?
from ...utils import invalid_char_reg  # invalid_char_reg imported but not used?
from ...utils import (  # subscribe_notifications imported but not used?
    load_json_body,
    subscribe_notifications,
)

HOUR = 60 * 60
WEEK = 24 * HOUR * 7
simple_ev_lengths = set([WEEK, 2 * WEEK])
simple_12hr_num_events = set([7, 14])

columns = {
    "id": "`schedule`.`id` as `id`",
    "roster": "`roster`.`name` as `roster`, `roster`.`id` AS `roster_id`",
    "roster_id": "`roster`.`id` AS `roster_id`",
    "auto_populate_threshold": "`schedule`.`auto_populate_threshold` as `auto_populate_threshold`",
    "role": "`role`.`name` as `role`, `role`.`id` AS `role_id`",
    "role_id": "`role`.`id` AS `role_id`",
    "team": "`team`.`name` as `team`, `team`.`id` AS `team_id`",
    "team_id": "`team`.`id` AS `team_id`",
    # schedule_event columns needed for the 'events' field
    "events": [
        # Concatenate start and duration, separate events with ';', start/duration with ','
        "GROUP_CONCAT(CONCAT(`schedule_event`.`start`, ',', `schedule_event`.`duration`) SEPARATOR ';') AS `event_data_concat`"
        # Note: We no longer select raw start, duration, schedule_event_id here when grouping
    ],
    "advanced_mode": "`schedule`.`advanced_mode` AS `advanced_mode`",
    "timezone": "`team`.`scheduling_timezone` AS `timezone`",
    "scheduler": "`scheduler`.`name` AS `scheduler_name`",  # Renamed alias to avoid collision
    "last_epoch_scheduled": "`schedule`.`last_epoch_scheduled`",
    "last_scheduled_user_id": "`schedule`.`last_scheduled_user_id`",
}

# Columns needed in the SELECT list when 'events' is requested, plus schedule.id for grouping
event_related_select_cols = [
    "`schedule`.`id`",
    columns["events"],
]
# Need base schedule columns plus JOINed table names for default all_columns
all_columns_select_parts = [
    columns["id"],
    columns["roster"],
    columns["auto_populate_threshold"],
    columns["role"],
    columns["team"],
    columns["advanced_mode"],
    columns["timezone"],
    columns["scheduler"],
]


constraints = {
    "id": "`schedule`.`id` = %s",
    "id__eq": "`schedule`.`id` = %s",
    "id__ge": "`schedule`.`id` >= %s",
    "id__gt": "`schedule`.`id` > %s",
    "id__le": "`schedule`.`id` <= %s",
    "id__lt": "`schedule`.`id` < %s",
    "id__ne": "`schedule`.`id` != %s",
    "name": "`roster`.`name` = %s",  # Constraint on roster name
    "name__contains": '`roster`.`name` LIKE CONCAT("%%", %s, "%%")',
    "name__endswith": '`roster`.`name` LIKE CONCAT("%%", %s)',
    "name__eq": "`roster`.`name` = %s",
    "name__startswith": '`roster`.`name` LIKE CONCAT(%s, "%%")',
    "role": "`role`.`name` = %s",  # Constraint on role name
    "role__contains": '`role`.`name` LIKE CONCAT("%%", %s, "%%")',
    "role__endswith": '`role`.`name` LIKE CONCAT("%%", %s)',
    "role__eq": "`role`.`name` = %s",
    "role__startswith": '`role`.`name` LIKE CONCAT(%s, "%%")',
    "team": "`team`.`name` = %s",  # Constraint on team name
    "team__contains": '`team`.`name` LIKE CONCAT("%%", %s, "%%")',
    "team__endswith": '`team`.`name` LIKE CONCAT("%%", %s)',
    "team__eq": "`team`.`name` = %s",
    "team__startswith": '`team`.`name` LIKE CONCAT(%s, "%%")',
    "team_id": "`schedule`.`team_id` = %s",
    "roster_id": "`schedule`.`roster_id` = %s",
    "role_id": "`schedule`.`role_id` = %s",
    "scheduler_id": "`schedule`.`scheduler_id` = %s",
}


def validate_simple_schedule(events):
    """
    Return boolean whether a schedule can be represented in simple mode. Simple schedules can have:
    1. One event that is one week long
    2. One event that is two weeks long
    3. Seven events that are 12 hours long
    4. Fourteen events that are 12 hours long
    """
    if len(events) == 1 and events[0]["duration"] in simple_ev_lengths:
        return True
    else:
        return len(events) in simple_12hr_num_events and all(
            [ev["duration"] == 12 * HOUR for ev in events]
        )


# This helper function correctly uses a passed cursor
def insert_schedule_events(schedule_id, events, cursor):
    """
    Helper to insert schedule events for a schedule
    """
    insert_events = """INSERT INTO `schedule_event` (`schedule_id`, `start`, `duration`)
                       VALUES (%(schedule)s, %(start)s, %(duration)s)"""
    # Merge consecutive events for db storage. This creates an equivalent, simpler
    # form of the schedule for the scheduler.
    raw_events = sorted(events, key=lambda e: e["start"])
    new_events = []
    for e in raw_events:
        # Ensure start and duration are not None before accessing
        current_start = e.get("start")
        current_duration = e.get("duration")
        if current_start is None or current_duration is None:
            # Basic validation, should ideally happen earlier
            raise ValueError(f"Event has missing start or duration: {e}")

        if len(new_events) > 0 and current_start == new_events[-1].get(
            "start", 0
        ) + new_events[-1].get(
            "duration", 0
        ):  # Use .get with defaults for safety
            new_events[-1]["duration"] += current_duration
        else:
            new_events.append(
                {"start": current_start, "duration": current_duration}
            )  # Ensure new dict

    events_for_executemany = []
    for e in new_events:
        # Prepare data for executemany, ensuring schedule_id is included
        events_for_executemany.append(
            {
                "schedule": schedule_id,
                "start": e["start"],
                "duration": e["duration"],
            }
        )

    if events_for_executemany:  # Only execute if there are events
        cursor.executemany(insert_events, events_for_executemany)
    # No close() needed here, the connection/cursor are managed by the caller.


# Your existing get_schedules function (corrected version from the previous turn)
def get_schedules(filter_params, dbinfo=None, fields=None):
    """
    Helper function to get schedule data for a request. Uses parameterized queries for safety.
    Can optionally use an existing connection/cursor from dbinfo.

    :param filter_params: dict mapping constraint keys with values. Valid constraints are
    defined in the global ``constraints`` dict.
    :param dbinfo: optional. If provided, defines (connection, cursor) to use in DB queries.
    Otherwise, this creates its own connection/cursor.
    :param fields: optional. If provided, defines which schedule fields to return. Valid
    fields are defined in the global ``columns`` dict. Defaults to all fields. Invalid
    fields raise a 400 Bad Request.
    :return:
    """
    # Use sets to track requested fields and needed joins to avoid duplicates
    requested_fields_set = set()
    required_joins = {}
    select_col_parts = []

    events_requested = False
    scheduler_requested = False  # Tracks if the 'scheduler' field is requested

    # Define mapping from requested field name to SQL column definition(s) and required join
    # This structure makes it explicit which fields require which joins and which columns
    field_mapping = {
        "id": {"cols": [columns["id"]]},
        "auto_populate_threshold": {
            "cols": [columns["auto_populate_threshold"]]
        },
        "advanced_mode": {"cols": [columns["advanced_mode"]]},
        "roster": {
            "cols": [columns["roster"], columns["roster_id"]],
            "join": "JOIN `roster` ON `roster`.`id` = `schedule`.`roster_id`",
        },
        "role": {
            "cols": [columns["role"], columns["role_id"]],
            "join": "JOIN `role` ON `role`.`id` = `schedule`.`role_id`",
        },
        "team": {  # Team name field
            "cols": [columns["team"], columns["team_id"]],
            "join": "JOIN `team` ON `team`.`id` = `schedule`.`team_id`",
        },
        "timezone": {  # Team timezone field
            "cols": [columns["timezone"]],
            "join": "JOIN `team` ON `team`.`id` = `schedule`.`team_id`",  # Uses the same team join
        },
        "scheduler": {
            "cols": [
                columns["scheduler"]
            ],  # Use the alias defined in columns dict
            "join": "JOIN `scheduler` ON `scheduler`.`id` = `schedule`.`scheduler_id`",
        },
        "events": {
            "cols": columns[
                "events"
            ],  # This should be a list of GROUP_CONCAT columns now
            "join": "LEFT JOIN `schedule_event` ON `schedule_event`.`schedule_id` = `schedule`.`id`",
        },
        # Add other basic schedule fields here if needed:
        # "last_epoch_scheduled": {"cols": [columns["last_epoch_scheduled"]]},
        # "last_scheduled_user_id": {"cols": [columns["last_scheduled_user_id"]]},
    }

    # Default to all fields if none are specified
    if fields is None:
        fields = list(field_mapping.keys())

    # Process requested fields
    for f in fields:
        if f not in field_mapping:
            raise HTTPBadRequest("Bad fields", f"Invalid field requested: {f}")
        if f in requested_fields_set:
            continue  # Skip if already processed (handles potential duplicates in input list)
        requested_fields_set.add(f)

        mapping = field_mapping[f]

        # Add columns from the mapping
        select_col_parts.extend(mapping.get("cols", []))

        # Add required join using the join string as the key in the dict to ensure uniqueness
        join_string = mapping.get("join")
        if join_string:
            required_joins[join_string] = (
                join_string  # Use the string as key and value
            )

        # Track special cases requiring post-processing or secondary queries
        if f == "events":
            events_requested = True
        if f == "scheduler":
            scheduler_requested = True

    # Ensure schedule.id is selected if not already requested (essential for grouping/joining later)
    # We need to check if the string "`schedule`.`id` AS `id`" is already in select_col_parts
    id_col_def = columns["id"]  # Use the defined column string
    if id_col_def not in select_col_parts:
        select_col_parts.insert(0, id_col_def)  # Add at the beginning

    # Build the SELECT clause
    cols_clause = ", ".join(select_col_parts)

    # Build the FROM clause from the base table and collected unique joins
    from_clause_parts = ["`schedule`"] + list(required_joins.keys())
    from_clause = " ".join(from_clause_parts)

    # *** SECURITY FIX: Use parameterized queries for the WHERE clause ***
    where_params_snippets = []  # e.g., "`roster`.`name` = %s"
    where_values = []  # e.g., ["roster-foo"]

    for key, value in filter_params.items():
        if key in constraints:
            where_params_snippets.append(constraints[key])
            where_values.append(value)  # Add value directly, no escape needed
        # else: Ignore unknown parameters

    where_clause = (
        " AND ".join(where_params_snippets) if where_params_snippets else "1"
    )  # Use "1" for no WHERE conditions

    # Construct the main query string template
    query_template = f"SELECT {cols_clause} FROM {from_clause}"
    if where_clause != "1":
        query_template += f" WHERE {where_clause}"

    # Add grouping if events are requested (because we used GROUP_CONCAT)
    # We group by the unique identifier of the schedule row.
    # Note: In strict SQL modes, you'd need to group by all non-aggregated SELECT columns.
    # Relying on MySQL's behavior of allowing grouping by PK when selecting other fields from the same row.
    # This is needed because GROUP_CONCAT is an aggregate function.
    if events_requested:
        query_template += (
            " GROUP BY `schedule`.`id`"  # Group by the primary key
        )

    # Add ordering for consistent results, especially with grouping
    query_template += " ORDER BY `schedule`.`id` ASC"  # Order by schedule id

    # *** Connection Management using 'with' or provided dbinfo ***
    data = []  # Initialize data outside the conditional block
    orders_data = []  # Initialize orders data
    connection_to_use = None  # Explicitly initialize to None
    cursor_to_use = None  # Explicitly initialize to None

    if dbinfo is None:
        # This function needs to open and manage the connection
        # print("get_schedules: dbinfo is None, opening new connection")
        connection_opened_here = True
        try:
            # Assuming db.connect returns a connection object that supports 'with' and .cursor()
            with db.connect() as connection:
                connection_to_use = connection
                # Assuming db.DictCursor provides dictionary-like row access
                cursor_to_use = connection.cursor(db.DictCursor)

                # *** EXECUTE MAIN QUERY with parameters ***
                # print(f"Main query: {query_template}") # Avoid logging template with raw %s in production logs
                print(f"get_schedules: Main query (template): {query_template}")
                print(f"get_schedules: Main query (values): {where_values}")
                cursor_to_use.execute(
                    query_template, where_values
                )  # Pass values as the second argument
                data = cursor_to_use.fetchall()

                # Execute secondary query for scheduler order if requested and data was found in the first query
                if scheduler_requested and data:
                    schedule_ids = {
                        d["id"] for d in data if d and "id" in d
                    }  # Safely get IDs
                    if (
                        schedule_ids
                    ):  # Only run if there are schedule IDs to fetch orders for
                        # print("get_schedules: Fetching scheduler order data...")
                        # Use the same cursor for the second query
                        cursor_to_use.execute(
                            """SELECT `schedule_id`, `user`.`name` FROM `schedule_order`
                                          JOIN `user` ON `user_id` = `user`.`id`
                                          WHERE `schedule_id` IN %s
                                          ORDER BY `schedule_id`,`priority`, `user_id`""",
                            (
                                tuple(schedule_ids),
                            ),  # Pass tuple for IN clause parameter
                        )
                        # Fetch orders data immediately after execution
                        orders_data = cursor_to_use.fetchall()
                        # print(f"get_schedules: Fetched {len(orders_data)} scheduler order rows.")

            # Connection and cursor are automatically closed by the 'with' block when it exits
            # print("get_schedules: Connection closed (opened here)")
            connection_to_use = None  # Ensure references are cleared
            cursor_to_use = None
            connection_opened_here = False  # Reset flag

        except Exception as e:
            # Log or handle exceptions during DB interaction
            print(
                f"Error in get_schedules (connection opened here): {e}"
            )  # Replace with proper logging
            # Consider logging the full query and values for debugging in development, but be cautious in production
            # print(f"Query: {query_template}")
            # print(f"Values: {where_values}")
            # Note: The 'with' block will handle connection cleanup even if an error occurs inside
            raise  # Re-raise the exception for the caller (on_get) to handle

    else:
        # Use the provided connection and cursor
        # print("get_schedules: dbinfo provided, using existing connection")
        connection_opened_here = False  # Not opened here
        connection_to_use, cursor_to_use = dbinfo
        # Ensure provided dbinfo contains a valid cursor object
        if cursor_to_use is None or connection_to_use is None:
            raise ValueError(
                "Invalid dbinfo provided: connection or cursor is None"
            )

        try:
            # *** EXECUTE MAIN QUERY with parameters ***
            # print(f"Main query (provided connection): {query_template}")
            # print(f"Main query (values): {where_values}")
            print(
                f"get_schedules: Main query (template, provided connection): {query_template}"
            )
            print(
                f"get_schedules: Main query (values, provided connection): {where_values}"
            )
            cursor_to_use.execute(
                query_template, where_values
            )  # Pass values as the second argument
            data = cursor_to_use.fetchall()

            # Execute secondary query for scheduler order if requested and data was found
            if scheduler_requested and data:
                schedule_ids = {
                    d["id"] for d in data if d and "id" in d
                }  # Safely get IDs
                if schedule_ids:  # Only run if there are schedule IDs
                    # print("get_schedules: Fetching scheduler order data (provided connection)...")
                    # Use the same provided cursor for the second query
                    cursor_to_use.execute(
                        """SELECT `schedule_id`, `user`.`name` FROM `schedule_order`
                                      JOIN `user` ON `user_id` = `user`.`id`
                                      WHERE `schedule_id` IN %s
                                      ORDER BY `schedule_id`,`priority`, `user_id`""",
                        (
                            tuple(schedule_ids),
                        ),  # Pass tuple for IN clause parameter
                    )
                    # Fetch orders data immediately after execution
                    orders_data = cursor_to_use.fetchall()
                    # print(f"get_schedules: Fetched {len(orders_data)} scheduler order rows (provided connection).")

            # Do NOT close connection/cursor here, they are managed by the caller (dbinfo provider)
        except Exception as e:
            # Log or handle exceptions during DB interaction
            print(
                f"Error in get_schedules (using provided connection): {e}"
            )  # Replace with proper logging
            # The caller's context manager will handle rollback/cleanup
            # Consider logging the full query and values for debugging in development, but be cautious in production
            # print(f"Query: {query_template}")
            # print(f"Values: {where_values}")
            raise  # Re-raise the exception

    # --- Post-processing logic ---
    # This logic operates on the 'data' list (results of the main query)
    # and 'orders_data' (results of the secondary query if run).

    final_data = []  # Build the final output structure
    data_by_id = {
        row["id"]: row for row in data if row and "id" in row
    }  # Map main data rows by schedule ID

    # Process schedule events if they were requested
    if events_requested:
        for schedule_id, schedule_row in list(
            data_by_id.items()
        ):  # Iterate copy as we modify dict
            # Get the concatenated event data string
            event_data_concat = schedule_row.pop(
                "event_data_concat", None
            )  # Remove from main row

            # Initialize events list for this schedule
            schedule_row["events"] = []

            # Parse the concatenated string and add events
            if event_data_concat:
                events_raw = str(event_data_concat).split(
                    ";"
                )  # Ensure it's a string before splitting
                for event_raw in events_raw:
                    parts = event_raw.split(
                        ","
                    )  # Split into start and duration
                    if len(parts) == 2:
                        try:
                            # Convert to int - handle potential errors
                            start = int(parts[0])
                            duration = int(parts[1])
                            schedule_row["events"].append(
                                {"start": start, "duration": duration}
                            )
                        except (ValueError, TypeError):
                            # Log a warning for malformed data
                            print(
                                f"Warning: Could not parse event data part: '{event_raw}' for schedule ID: {schedule_id}"
                            )
                        except Exception as parse_e:
                            print(
                                f"Unexpected error parsing event data part '{event_raw}': {parse_e}"
                            )

    # Format scheduler order data if it was requested
    if (
        scheduler_requested and orders_data
    ):  # orders_data is populated if scheduler was requested and main query returned data
        orders_by_schedule = defaultdict(list)
        for row in orders_data:
            orders_by_schedule[row["schedule_id"]].append(row["name"])

        # Attach orders to the correct schedule dictionaries in the main data_by_id dict
        for schedule_id, order_list in orders_by_schedule.items():
            if schedule_id in data_by_id:
                schedule_dict = data_by_id[schedule_id]
                # Assuming 'scheduler_name' holds the scheduler name alias from the SELECT
                # Replicate the original desired structure: {'name': scheduler_name, 'data': order_list}
                # The scheduler name is already in the main schedule_dict
                schedule_dict["scheduler"] = {
                    "name": schedule_dict.get(
                        "scheduler_name"
                    ),  # Get the name using its alias
                    "data": order_list,
                }
                # Remove the raw 'scheduler_name' key from the top level if it's only used inside 'scheduler'
                schedule_dict.pop("scheduler_name", None)
            else:
                # This case indicates an issue: got order data for a schedule not in the main result
                print(
                    f"Warning: Scheduler order data found for schedule ID {schedule_id} not in main query results."
                )

    # The data_by_id dictionary now contains the final structure for each schedule.
    # Convert the dictionary values back to a list for the final return value.
    final_data = list(data_by_id.values())

    return final_data


# on_get calls get_schedules, so it doesn't manage the connection
def on_get(req, resp, team, roster):
    """
    Get schedules for a given roster. Information on schedule attributes is detailed
    in the schedules POST endpoint documentation. Schedules can be filtered with
    the following parameters passed in the query string:

    ... (docstring remains the same) ...
    """
    team_name = unquote(team)  # Renamed variable
    roster_name = unquote(roster)  # Renamed variable
    fields = req.get_param_as_list("fields")
    if not fields:
        # Use a list of logical field names, not raw column parts
        fields = list(columns.keys())

    params = req.params
    params["team"] = team_name  # Use variable
    params["roster"] = roster_name  # Use variable
    # Call get_schedules, which now handles its own connection or uses dbinfo (not used here)
    data = get_schedules(params, fields=fields)

    resp.text = json_dumps(data)


required_params = frozenset(["events", "role", "advanced_mode"])

# Define required parameters for the SQL INSERT query placeholders
sql_insert_params = {
    "roster",
    "team",
    "role",
    "auto_populate_threshold",
    "advanced_mode",
    "scheduler_name",
}


@login_required
def on_post(req, resp, team, roster):
    """
    Schedule create endpoint. Schedules are templates for the auto-scheduler to follow that define
    how it should populate a certain period of time. This template is followed repeatedly to
    populate events on a team's calendar. Schedules are associated with a roster, which defines
    the pool of users that the scheduler selects from. Similarly, the schedule's role indicates
    the role that the populated events shoud have. The ``auto_populate_threshold`` parameter
    defines how far into the future the scheduler populates.

    ... (rest of docstring unchanged) ...
    """
    # Load and initially process data
    try:
        data = load_json_body(req)
        data["team"] = unquote(team)
        data["roster"] = unquote(roster)
    except Exception as e:
        raise HTTPBadRequest(
            "Invalid Request",
            f"Failed to process request body or URL parameters: {e}",
        )

    # Auth check using URL team parameter
    check_team_auth(data["team"], req)

    # --- Validation ---
    # Check for required fields in the request body
    missing_params = required_params - set(data.keys())
    if missing_params:
        raise HTTPBadRequest(
            "invalid schedule",
            f"missing required parameters: {', '.join(missing_params)}",
        )

    # Validate events structure and content
    schedule_events = data.get("events")
    if not isinstance(schedule_events, list):
        raise HTTPBadRequest("invalid schedule", "events must be a list")
    if not schedule_events:
        raise HTTPBadRequest("invalid schedule", "events list cannot be empty")

    for idx, sev in enumerate(schedule_events):
        if not isinstance(sev, dict):
            raise HTTPBadRequest(
                "invalid schedule", f"event at index {idx} is not an object"
            )
        if "start" not in sev or "duration" not in sev:
            raise HTTPBadRequest(
                "invalid schedule",
                f"schedule event at index {idx} requires both start and duration fields",
            )
        if not isinstance(sev["start"], (int, float)):
            raise HTTPBadRequest(
                "invalid schedule",
                f"schedule event start at index {idx} must be a number",
            )
        if (
            not isinstance(sev["duration"], (int, float))
            or sev["duration"] <= 0
        ):
            raise HTTPBadRequest(
                "invalid schedule",
                f"schedule event duration at index {idx} must be a positive number",
            )

    # Validate and normalize advanced_mode
    advanced_mode_raw = data.get(
        "advanced_mode", False
    )  # Default to False if missing
    if isinstance(advanced_mode_raw, int) and advanced_mode_raw in {0, 1}:
        advanced_mode = bool(advanced_mode_raw)
    elif isinstance(advanced_mode_raw, bool):
        advanced_mode = advanced_mode_raw
    else:
        raise HTTPBadRequest(
            "invalid schedule",
            "advanced_mode must be a boolean (true/false) or 0/1",
        )
    data["advanced_mode"] = (
        1 if advanced_mode else 0
    )  # Store as int 0 or 1 for DB

    # Validate simple schedule consistency if advanced_mode is False
    if not advanced_mode:
        if not validate_simple_schedule(schedule_events):
            raise HTTPBadRequest(
                "invalid schedule",
                "Provided events are not valid for simple mode (advanced_mode=false)",
            )

    # Validate and normalize auto_populate_threshold
    threshold_raw = data.get("auto_populate_threshold", 21)  # Default to 21
    try:
        threshold = int(threshold_raw)
        if threshold < 0:
            raise ValueError("Threshold cannot be negative")
        data["auto_populate_threshold"] = threshold
    except (ValueError, TypeError):
        raise HTTPBadRequest(
            "invalid schedule",
            "auto_populate_threshold must be a non-negative integer",
        )

    # Extract scheduler info safely
    scheduler_info = data.get("scheduler", {})  # Use empty dict if missing
    data["scheduler_name"] = scheduler_info.get(
        "name", "default"
    )  # Default scheduler name
    scheduler_data_list = scheduler_info.get("data")  # Might be None or a list

    # Validate scheduler_data if scheduler is round-robin
    if data["scheduler_name"] == "round-robin":
        if not isinstance(scheduler_data_list, list) or not all(
            isinstance(u, str) for u in scheduler_data_list
        ):
            raise HTTPBadRequest(
                "invalid schedule",
                "scheduler.data must be a list of usernames for round-robin scheduler",
            )
    # --- End Validation ---

    # --- Database Operations ---
    # Define the INSERT statement (add LIMIT 1 to subqueries for safety)
    insert_schedule_sql = """
        INSERT INTO `schedule` (
            `roster_id`, `team_id`, `role_id`, `auto_populate_threshold`,
            `advanced_mode`, `scheduler_id`
        ) VALUES (
            (SELECT `roster`.`id` FROM `roster` JOIN `team` ON `roster`.`team_id` = `team`.`id`
             WHERE `roster`.`name` = %(roster)s AND `team`.`name` = %(team)s LIMIT 1),
            (SELECT `id` FROM `team` WHERE `name` = %(team)s LIMIT 1),
            (SELECT `id` FROM `role` WHERE `name` = %(role)s LIMIT 1),
            %(auto_populate_threshold)s,
            %(advanced_mode)s,
            (SELECT `id` FROM `scheduler` WHERE `name` = %(scheduler_name)s LIMIT 1)
        )"""

    # Prepare parameters strictly for the INSERT query
    insert_params = {key: data[key] for key in sql_insert_params if key in data}

    schedule_id = None  # Initialize schedule_id

    try:
        with db.connect() as connection:
            cursor = connection.cursor(db.DictCursor)  # Using DictCursor
            try:
                # Execute the main INSERT for the schedule table
                cursor.execute(insert_schedule_sql, insert_params)

                schedule_id = cursor.lastrowid
                if schedule_id is None:
                    raise HTTPInternalServerError(  # Changed from HTTPError
                        title="Database Error",
                        description="Failed to retrieve new schedule ID after insert.",
                    )

                # Insert schedule events using the helper function
                insert_schedule_events(schedule_id, schedule_events, cursor)

                # If scheduler is round-robin, insert schedule_order entries
                if (
                    data["scheduler_name"] == "round-robin"
                    and scheduler_data_list
                ):
                    order_params = [
                        (schedule_id, name, idx)
                        for idx, name in enumerate(scheduler_data_list)
                    ]
                    if order_params:  # Only execute if there are users
                        cursor.executemany(
                            """INSERT INTO `schedule_order` (`schedule_id`, `user_id`, `priority`)
                               VALUES (%s, (SELECT `id` FROM `user` WHERE `name` = %s LIMIT 1), %s)""",
                            order_params,
                        )

                # Commit the transaction
                connection.commit()

            except db.IntegrityError as e:
                connection.rollback()  # Rollback on integrity error
                err_msg = str(e.args[1]) if len(e.args) > 1 else str(e)
                # Provide clearer messages based on likely FK failures
                if "Column 'roster_id' cannot be null" in err_msg:
                    err_msg = f'Roster "{data.get("roster")}" not found for team "{data.get("team")}".'
                elif "Column 'role_id' cannot be null" in err_msg:
                    err_msg = f'Role "{data.get("role")}" not found.'
                elif "Column 'scheduler_id' cannot be null" in err_msg:
                    err_msg = (
                        f'Scheduler "{data.get("scheduler_name")}" not found.'
                    )
                elif "Column 'team_id' cannot be null" in err_msg:
                    # Should be caught by team auth/subquery, but good to have
                    err_msg = f'Team "{data.get("team")}" not found.'
                elif "Duplicate entry" in err_msg:
                    # Adjust if there's a unique constraint (e.g., roster+role?)
                    err_msg = f"A schedule with similar properties might already exist: {err_msg}"
                else:
                    err_msg = f"Database Integrity Error: {err_msg}"  # Generic fallback
                raise HTTPError(
                    "422 Unprocessable Entity", "IntegrityError", err_msg
                ) from e

            except Exception as e:
                connection.rollback()  # Rollback on any other error within transaction
                print(
                    f"Error during schedule creation transaction: {e}"
                )  # Log error
                raise HTTPInternalServerError(
                    description=f"Failed to create schedule: {e}"
                ) from e

    except db.Error as e:
        print(f"Database connection error in on_post schedule: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise known HTTP errors (from validation or IntegrityError handler)
        raise e
    except Exception as e:
        # Catch unexpected errors (e.g., during initial processing)
        print(f"Unexpected error in on_post schedule: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # If successful, return 201 Created with the new ID
    resp.status = HTTP_201
    resp.text = json_dumps({"id": schedule_id})

