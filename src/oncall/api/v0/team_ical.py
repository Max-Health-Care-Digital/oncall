# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time

from ... import db
from . import ical
from .roles import get_role_ids  # Assuming get_role_ids takes a cursor


def get_team_events(team, start, roles=None, include_subscribed=False):
    """
    Get team events for iCal feed. Uses parameterized queries for safety.

    :param team: Team name
    :param start: Unix timestamp for the minimum event end time
    :param roles: Optional list of role names to filter by
    :param include_subscribed: Whether to include events from subscribed teams
    :return: List of event dictionaries
    """
    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        cursor = connection.cursor(db.DictCursor)

        # Build base query template
        query_template_base = """
            SELECT
                `event`.`id`,
                `team`.`name` AS team,
                `user`.`name` AS user,
                `role`.`name` AS role,
                `event`.`start`,
                `event`.`end`
            FROM `event`
                JOIN `team` ON `event`.`team_id` = `team`.`id`
                JOIN `user` ON `event`.`user_id` = `user`.`id`
                JOIN `role` ON `event`.`role_id` = `role`.`id`
            WHERE `event`.`end` > %s
        """

        # List to hold WHERE clause snippets and their corresponding values
        conditions = []
        values = []

        # 1. Add event.end > %s condition (already in base template, just add value)
        values.append(start)

        # 2. Build Team/Subscription conditions
        team_or_subs_conditions_snippets = []  # Snippets for the OR group
        team_or_subs_values = []  # Values for the OR group

        # Add base team.name = %s condition to the OR group
        team_or_subs_conditions_snippets.append("`team`.`name` = %s")
        team_or_subs_values.append(team)

        # If including subscriptions, fetch subscription details
        if include_subscribed:
            # Fetch subscriptions using a separate query with its own parameter
            # This query is inside the main 'with' block, uses the same cursor.
            # Ensure this query is also parameterized correctly.
            cursor.execute(
                """SELECT `subscription_id`, `role_id`
                   FROM `team_subscription`
                   JOIN `team` ON `team_id` = `team`.`id`
                   WHERE `team`.`name` = %s""",
                (team,),  # Parameterize team name
            )
            subs_results = cursor.fetchall()  # Fetch all results

            # Add subscription conditions to the OR group
            # *** FIX: Add subscription IDs and Role IDs as PARAMETERS, not formatted into the string ***
            for row in subs_results:
                # Add snippet for each subscription: (team.id = %s AND role.id = %s)
                team_or_subs_conditions_snippets.append(
                    "(`team`.`id` = %s AND `role`.`id` = %s)"
                )
                # Add the values for the parameters in the snippet
                team_or_subs_values.extend(
                    [row.get("subscription_id"), row.get("role_id")]
                )  # Use .get for safety

        # Combine team/subscription conditions into a single group if needed
        if team_or_subs_conditions_snippets:
            # Combine snippets with OR, wrap in parentheses
            team_subs_group_clause = (
                "(" + " OR ".join(team_or_subs_conditions_snippets) + ")"
            )
            conditions.append(
                team_subs_group_clause
            )  # Add the group to the main conditions
            values.extend(
                team_or_subs_values
            )  # Add ALL values for this OR group to the main values list
        # else: If no base team condition and no subscriptions (unlikely if team exists),
        # the list will be empty. The final WHERE clause will handle this.

        # 3. Add role condition if roles are provided
        if roles:
            # Get role IDs using the helper function and the current cursor
            role_ids = get_role_ids(cursor, roles)
            if role_ids:
                # *** FIX: Use IN (%s, %s, ...) format with a tuple parameter ***
                # Build the correct number of %s placeholders for the IN clause
                role_placeholders = ", ".join(["%s"] * len(role_ids))
                role_condition_snippet = f"`event`.`role_id` IN ({role_placeholders})"  # Snippet with placeholders
                conditions.append(
                    role_condition_snippet
                )  # Add the role condition
                values.extend(role_ids)  # Add the role ID values

        # Combine all conditions into the final WHERE clause string template
        final_where_clause = (
            " AND ".join(conditions) if conditions else "1"
        )  # Use "1" for no WHERE conditions

        # Construct the final query string template
        final_query_template = query_template_base + (
            " AND " + final_where_clause if final_where_clause != "1" else ""
        )

        # Optional: Add ordering for consistent results
        final_query_template += " ORDER BY `event`.`start` ASC"

        # *** EXECUTE FINAL QUERY with ALL collected parameters ***
        # The values list now contains: [start, team (base), sub_id_1, role_id_1, sub_id_2, role_id_2, ..., role_id_a, role_id_b, ...]
        cursor.execute(
            final_query_template, values
        )  # Pass ALL collected values

        # Fetch all events
        events = cursor.fetchall()

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    return events


# on_get calls get_team_events, so it doesn't manage the connection
def on_get(req, resp, team):
    """
    Get ics file for a given team's on-call events. Gets all events starting
    after the optional "start" parameter, which defaults to the current
    time. If defined, start should be a Unix timestamp in seconds.

    **Example request:**

    .. sourcecode:: http

        GET /api/v0/teams/test-team/ical?start=12345 HTTP/1.1
        Content-Type: text/calendar

        BEGIN:VCALENDAR
        ...
    """
    # Get parameters, providing defaults
    start = req.get_param_as_int("start")
    if start is None:
        start = int(time.time())

    # contact parameter used in ical.events_to_ical, not DB query
    contact = req.get_param_as_bool("contact", default=True)

    # roles parameter for filtering
    roles = req.get_param_as_list("roles")

    # include_subscribed parameter for filtering
    include_sub = req.get_param_as_bool("include_subscribed", default=True)

    # Call get_team_events, which now handles its own connection management
    events = get_team_events(
        team, start, roles=roles, include_subscribed=include_sub
    )

    # Generate iCal response
    resp.text = ical.events_to_ical(events, team, contact)
    resp.set_header("Content-Type", "text/calendar")
