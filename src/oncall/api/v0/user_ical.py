# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time

from falcon import HTTPInternalServerError  # Added for error handling example

from ... import db

# Assuming login_required is appropriately defined/imported elsewhere for on_get
from ...auth import login_required
from . import ical
from .roles import get_role_ids
from .teams import get_team_ids


def get_user_events(user_name, start, roles=None, excluded_teams=None):
    """
    Fetches event details for a specific user from the database.

    Args:
        user_name (str): The name of the user whose events are to be fetched.
        start (int): Unix timestamp; only events ending after this time are included.
        roles (list[str], optional): List of role names to filter events by. Defaults to None.
        excluded_teams (list[str], optional): List of team names whose events should be excluded.
                                               Defaults to None.

    Returns:
        list[dict]: A list of event dictionaries fetched from the database.

    Raises:
        HTTPInternalServerError: If a database or unexpected error occurs.
        RuntimeError: If db.DictCursor is not available.
    """
    events = []  # Initialize to ensure it's defined in case of early error
    try:
        with db.connect() as connection:
            # Ensure DictCursor is available (good practice check)
            if not hasattr(db, "DictCursor") or not db.DictCursor:
                raise RuntimeError(
                    "DictCursor is required but not available. Check DBAPI driver and db.init()."
                )
            cursor = connection.cursor(db.DictCursor)

            # NOTE: Parameterizing variable-length IN clauses can be complex depending on DBAPI driver.
            # This code formats numeric IDs fetched safely via get_role_ids/get_team_ids into the query.
            # Ensure get_role_ids/get_team_ids handle their input *names* safely (e.g., using parameters).
            role_condition = ""
            # Pass the cursor to helper functions needing database access within the same transaction context
            role_ids = get_role_ids(cursor, roles)
            if role_ids:
                # Formatting safe numeric IDs into the query string
                role_condition = " AND `event`.`role_id` IN ({0})".format(
                    ",".join(map(str, role_ids))
                )

            excluded_teams_condition = ""
            excluded_team_ids = get_team_ids(
                cursor, excluded_teams
            )  # Pass cursor
            if excluded_team_ids:
                # Formatting safe numeric IDs into the query string
                excluded_teams_condition = (
                    " AND `event`.`team_id` NOT IN ({0})".format(
                        ",".join(map(str, excluded_team_ids))
                    )
                )

            # Construct the final query
            query = (
                """
                SELECT
                    `event`.`id`,
                    `team`.`name` AS team,
                    `user`.`name` AS user,
                    `role`.`name` AS role,
                    `event`.`start`,
                    `event`.`end`
                FROM `event`
                    JOIN `team` ON `event`.`team_id` = `team`.`id`
                    JOIN `user` ON `event`.`user_id = `user`.`id`
                    JOIN `role` ON `event`.`role_id` = `role`.`id`
                WHERE
                    `event`.`end` > %s AND
                    `user`.`name` = %s
                """
                + role_condition
                + excluded_teams_condition
                # Consider adding ORDER BY if consistent ordering is needed
                # ORDER BY `event`.`start` ASC
            )

            # Execute the query with parameters for WHERE clause
            cursor.execute(query, (start, user_name))
            events = cursor.fetchall()

            # No need for explicit cursor.close() or connection.close()
            # The 'with' block handles resource cleanup automatically.

    except db.Error as e:
        # Log the specific database error
        print(f"Database error occurred in get_user_events: {e}")
        # Raise an error that the API layer can catch and convert to an HTTP response
        raise HTTPInternalServerError(
            description="Failed to retrieve user events due to a database error."
        )
    except Exception as e:
        # Log any other unexpected errors
        print(f"Unexpected error occurred in get_user_events: {e}")
        raise HTTPInternalServerError(
            description="An unexpected error occurred while retrieving user events."
        )

    return events


@login_required
def on_get(req, resp, user_name):
    """
    Get ics file for a given user's on-call events. Gets all events starting
    after the optional "start" parameter, which defaults to the current
    time. If defined, start should be a Unix timestamp in seconds.

    **Example request:**

    .. sourcecode:: http

        GET /api/v0/users/jdoe/ical HTTP/1.1
        Content-Type: text/calendar

        BEGIN:VCALENDAR
        ...

    """
    start = req.get_param_as_int("start")
    if start is None:
        # Default to current time if 'start' param is missing
        start = int(time.time())

    contact = req.get_param_as_bool("contact")
    if contact is None:
        # Default to including contact info if 'contact' param is missing
        contact = True

    roles = req.get_param_as_list("roles")
    excluded_teams = req.get_param_as_list("excludedTeams")

    # Call the refactored helper function to get events
    # Exceptions raised in get_user_events will propagate up unless caught here
    events = get_user_events(
        user_name, start, roles=roles, excluded_teams=excluded_teams
    )

    # Generate the iCalendar file using the fetched events
    # Assumes ical.events_to_ical handles its own errors or an empty events list
    resp.text = ical.events_to_ical(events, user_name, contact)
    resp.set_header("Content-Type", "text/calendar")
    # Optional: Set a filename for download
    # resp.set_header('Content-Disposition', f'attachment; filename="{user_name}_oncall.ics"')
