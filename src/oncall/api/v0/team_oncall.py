# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import db


def on_get(req, resp, team, role=None):
    """
    Get current active event for team based on given role.

    **Example request**:

    .. sourcecode:: http

        GET /api/v0/teams/team_ops/oncall/primary HTTP/1.1
        Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
         {
           "user": "foo",
           "start": 1487426400,
           "end": 1487469600,
           "full_name": "Foo Icecream",
           "contacts": {
             "im": "foo",
             "sms": "+1 123-456-7890",
             "email": "foo@example.com",
             "call": "+1 123-456-7890"
           }
         },
         {
           "user": "bar",
           "start": 1487426400,
           "end": 1487469600,
           "full_name": "Bar Dog",
           "contacts": {
             "im": "bar",
             "sms": "+1 123-456-7890",
             "email": "bar@example.com",
             "call": "+1 123-456-7890"
           }
         }
        ]

    :statuscode 200: no error
    """
    get_oncall_query = """
        SELECT `user`.`full_name` AS `full_name`,
               `event`.`start`, `event`.`end`,
               `contact_mode`.`name` AS `mode`,
               `user_contact`.`destination`,
               `user`.`name` AS `user`,
               `team`.`name` AS `team`,
               `role`.`name` AS `role`
        FROM `event`
        JOIN `user` ON `event`.`user_id` = `user`.`id`
        JOIN `team` ON `event`.`team_id` = `team`.`id`
        JOIN `role` ON `role`.`id` = `event`.`role_id`
        LEFT JOIN `team_subscription` ON `subscription_id` = `team`.`id`
            AND `team_subscription`.`role_id` = `role`.`id`
        LEFT JOIN `team` `subscriber` ON `subscriber`.`id` = `team_subscription`.`team_id`
        LEFT JOIN `user_contact` ON `user`.`id` = `user_contact`.`user_id`
        LEFT JOIN `contact_mode` ON `contact_mode`.`id` = `user_contact`.`mode_id`
        WHERE UNIX_TIMESTAMP() BETWEEN `event`.`start` AND `event`.`end`
          AND (`team`.`name` = %s OR `subscriber`.`name` = %s)"""
    query_params = [team, team]
    if role is not None:
        get_oncall_query += " AND `role`.`name` = %s"
        query_params.append(role)

    fetched_data = []
    override_number = None

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Ensure DictCursor is available
        if not db.DictCursor:
            raise RuntimeError(
                "DictCursor is required but not available. Check DBAPI driver and db.init()."
            )
        cursor = connection.cursor(db.DictCursor)

        # Execute the main query to get on-call info
        cursor.execute(
            get_oncall_query, tuple(query_params)
        )  # Pass params as tuple
        fetched_data = cursor.fetchall()

        # Execute the query to get the override number
        cursor.execute(
            "SELECT `override_phone_number` FROM team WHERE `name` = %s",
            (team,),  # Pass team name as a tuple
        )
        team_info = cursor.fetchone()  # Use different variable name
        override_number = (
            team_info["override_phone_number"] if team_info else None
        )

        # No need for explicit cursor.close() or connection.close()
        # Connection is automatically released when exiting the 'with' block

    # Process the data fetched from the database *after* the connection is closed
    ret = {}
    for row in fetched_data:
        user = row["user"]
        # Add data row into accumulator only if not already there
        if user not in ret:
            # Copy the row to avoid modifying the original fetched data structure if needed elsewhere
            ret[user] = row.copy()  # Use copy() for safety
            ret[user]["contacts"] = {}
        mode = row.get("mode")  # Use .get() for safety with LEFT JOINs
        dest = row.get("destination")  # Use .get() for safety
        if mode and dest:  # Only add contact if both mode and destination exist
            ret[user]["contacts"][mode] = dest
        # Clean up keys potentially added to ret[user] that aren't needed in final output per example
        # (mode/destination were originally popped, implies they aren't wanted directly on the event)
        ret[user].pop("mode", None)
        ret[user].pop("destination", None)

    # Convert processed data back to a list
    processed_data = list(ret.values())

    # Apply override number if applicable
    for event in processed_data:
        # Ensure 'contacts' dict exists and role matches before applying override
        if (
            override_number
            and event.get("role") == "primary"
            and "contacts" in event
        ):
            event["contacts"]["call"] = override_number
            event["contacts"]["sms"] = override_number

    resp.text = json_dumps(processed_data)
