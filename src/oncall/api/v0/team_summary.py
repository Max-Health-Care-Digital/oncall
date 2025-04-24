# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from collections import defaultdict

from falcon import HTTPNotFound
from ujson import dumps

from ... import db  # Import the db module


def on_get(req, resp, team):
    """
    Endpoint to get a summary of the team's oncall information. Returns an object
    containing the fields ``current`` and ``next``, which then contain information
    on the current and next on-call shifts for this team. ``current`` and ``next``
    are objects keyed by role (if an event of that role exists), with values of
    lists of event/user information. This list will have multiple elements if
    multiple events with the same role are currently occurring, or if multiple
    events with the same role are starting next in the future at the same time.

    If no event with a given role exists, that role is excluded from the ``current``
    or ``next`` object. If no events exist, the ``current`` and ``next`` objects
    will be empty objects.

    **Example request:**

    .. sourcecode:: http

        GET api/v0/teams/team-foo/summary   HTTP/1.1
        Content-Type: application/json

    **Example response:**

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "current": { ... },
            "next": { ... }
        }

    """
    payload = {}
    override_num = None
    team_id = None
    users = set()
    contacts = []

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor from the connection wrapper
        if not db.DictCursor:
            # Raise a clearer error if DictCursor is essential and unavailable
            raise RuntimeError(
                "DictCursor is required but not available. Check DBAPI driver and db.init()."
            )
        cursor = connection.cursor(db.DictCursor)

        cursor.execute(
            "SELECT `id`, `override_phone_number` FROM `team` WHERE `name` = %s",
            (team,),  # Pass parameters as a tuple
        )
        if cursor.rowcount < 1:
            raise HTTPNotFound(description=f"Team '{team}' not found")

        data = cursor.fetchone()
        team_id = data["id"]
        override_num = data["override_phone_number"]

        # --- Current Query ---
        current_query = """
            SELECT `user`.`full_name` AS `full_name`,
                   `user`.`photo_url`,
                   `event`.`start`, `event`.`end`,
                   `event`.`user_id`,
                   `user`.`name` AS `user`,
                   `team`.`name` AS `team`,
                   `role`.`name` AS `role`
            FROM `event`
            JOIN `user` ON `event`.`user_id` = `user`.`id`
            JOIN `team` ON `event`.`team_id` = `team`.`id`
            JOIN `role` ON `role`.`id` = `event`.`role_id`
            WHERE UNIX_TIMESTAMP() BETWEEN `event`.`start` AND `event`.`end`"""

        # --- Build base WHERE clause and params for team/subscriptions ---
        team_where_clause_base = "`team`.`id` = %s"
        params = [team_id]  # Start parameter list with team_id

        # Fetch subscriptions (using a separate cursor or reusing is fine)
        # Reusing the main cursor here for simplicity
        cursor.execute(
            """SELECT `subscription_id`, `role_id` FROM `team_subscription`
                WHERE `team_id` = %s""",  # Parameterize the team_id here too
            (team_id,),
        )
        subscriptions = cursor.fetchall()

        team_where_sql = team_where_clause_base  # Start with the base clause
        if subscriptions:
            subscription_clauses = []
            for row in subscriptions:
                # Add placeholders and parameters for each subscription
                subscription_clauses.append(
                    "(`event`.`team_id` = %s AND `event`.`role_id` = %s)"
                )
                params.extend([row["subscription_id"], row["role_id"]])

            # Combine the base team_where with the subscription clauses using OR
            # Ensure event table alias is used for subscription part
            team_where_sql = f"({team_where_clause_base} OR ({' OR '.join(subscription_clauses)}))"
            # Note: params list already contains all necessary parameters in order

        # Execute the current query
        # Need to use `team_where_sql` but ensure table aliases match the query
        # The 'current_query' joins 'team', so `team.id` is valid there.
        # The subscription part uses `event.team_id`, also valid via JOINs.
        # Let's adjust the generated team_where_sql to be safe for `current_query`:
        current_team_where_sql = (
            team_where_clause_base  # Start with base `team.id = %s`
        )
        current_params = [team_id]
        if subscriptions:
            current_subscription_clauses = []
            for row in subscriptions:
                current_subscription_clauses.append(
                    "(`event`.`team_id` = %s AND `event`.`role_id` = %s)"
                )
                current_params.extend([row["subscription_id"], row["role_id"]])
            # Use team.id for base, event.team_id for subscriptions
            current_team_where_sql = f"({team_where_clause_base} OR ({' OR '.join(current_subscription_clauses)}))"

        cursor.execute(
            f"{current_query} AND ({current_team_where_sql})",
            tuple(current_params),
        )

        payload["current"] = defaultdict(list)
        for event in cursor:
            payload["current"][event["role"]].append(event)
            users.add(event["user_id"])

        # --- Next Query ---
        # Rebuild WHERE clause structure specifically for the subquery context
        # Inside the subquery, both `team.id` and `event.team_id` are potentially available
        subquery_team_where_clause_base = (
            "`team`.`id` = %s"  # Use team.id as subquery joins team
        )
        subquery_params = [team_id]
        subquery_team_where_sql = subquery_team_where_clause_base
        if subscriptions:
            subquery_subscription_clauses = []
            for row in subscriptions:
                # Use event.team_id here as it's clearer within the event context
                subquery_subscription_clauses.append(
                    "(`event`.`team_id` = %s AND `event`.`role_id` = %s)"
                )
                subquery_params.extend([row["subscription_id"], row["role_id"]])
            subquery_team_where_sql = f"({subquery_team_where_clause_base} OR ({' OR '.join(subquery_subscription_clauses)}))"
        # `subquery_params` now holds parameters for the subquery's WHERE clause.

        # Construct the next_query WITHOUT the final erroneous WHERE clause
        next_query = f"""
            SELECT `role`.`name` AS `role`,
                   `user`.`full_name` AS `full_name`,
                   `event`.`start`,
                   `event`.`end`,
                   `user`.`photo_url`,
                   `user`.`name` AS `user`,
                   `event`.`user_id`,
                   `event`.`role_id`,
                   `event`.`team_id`
            FROM `event`
            JOIN `role` ON `event`.`role_id` = `role`.`id`
            JOIN `user` ON `event`.`user_id` = `user`.`id`
            JOIN (
                SELECT `event`.`role_id`, `event`.`team_id`, MIN(`event`.`start` - UNIX_TIMESTAMP()) AS dist
                FROM `event` JOIN `team` ON `team`.`id` = `event`.`team_id`
                WHERE `start` > UNIX_TIMESTAMP() AND ({subquery_team_where_sql})  -- Inject the subquery where clause structure
                GROUP BY `event`.`role_id`, `event`.`team_id`
            ) AS t1
              ON `event`.`role_id` = `t1`.`role_id`
                 AND `event`.`start` - UNIX_TIMESTAMP() = `t1`.dist
                 AND `event`.`team_id` = `t1`.`team_id`
            -- REMOVED final WHERE clause here --
        """
        # Execute using only the parameters required for the subquery's WHERE clause
        cursor.execute(
            next_query, tuple(subquery_params)
        )  # Use subquery_params

        payload["next"] = defaultdict(list)
        for event in cursor:
            payload["next"][event["role"]].append(event)
            users.add(event["user_id"])

        # --- Contacts Query ---
        if users:
            placeholders = "%s"  # Single placeholder for the tuple

            contacts_query = f"""
                SELECT `contact_mode`.`name` AS `mode`,
                       `user_contact`.`destination`,
                       `user_contact`.`user_id`
                FROM `user`
                    JOIN `user_contact` ON `user`.`id` = `user_contact`.`user_id`
                    JOIN `contact_mode` ON `contact_mode`.`id` = `user_contact`.`mode_id`
                WHERE `user`.`id` IN ({placeholders})"""

            cursor.execute(contacts_query, (tuple(users),))
            contacts = cursor.fetchall()

            # Populate contacts
            for part in payload.values():
                for event_list in part.values():
                    for event in event_list:
                        event["user_contacts"] = {
                            c["mode"]: c["destination"]
                            for c in contacts
                            if c["user_id"] == event["user_id"]
                        }

        # Connection released automatically by 'with' block exit

    # --- Post-Connection Processing ---
    if override_num:
        try:
            if "primary" in payload.get("current", {}):
                for event in payload["current"]["primary"]:
                    if "user_contacts" in event:
                        event["user_contacts"]["call"] = override_num
                        event["user_contacts"]["sms"] = override_num
                    else:
                        event["user_contacts"] = {
                            "call": override_num,
                            "sms": override_num,
                        }
        except KeyError:
            pass  # Maintain original behavior

    resp.text = dumps(payload)
