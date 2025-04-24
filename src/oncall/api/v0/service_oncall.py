# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import db


def on_get(req, resp, service):
    """
    Get the current user on-call for a given service/role. Returns event start/end, contact info,
    and user name.

    **Example request**

    .. sourcecode:: http

        GET /api/v0/services/service-foo/oncall/primary  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "contacts": {
                    "call": "+1 111-111-1111",
                    "email": "jdoe@example.com",
                    "im": "jdoe",
                    "sms": "+1 111-111-1111"
                },
                "end": 1495695600,
                "start": 1495263600,
                "user": "John Doe"
            }
        ]

    """
    get_oncall_query_template = """
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
        LEFT JOIN `team_subscription` ON `team_subscription`.`subscription_id` = `team`.`id`
            AND `team_subscription`.`role_id` = `role`.`id`
        LEFT JOIN `user_contact` ON `user`.`id` = `user_contact`.`user_id`
        LEFT JOIN `contact_mode` ON `contact_mode`.`id` = `user_contact`.`mode_id`
        WHERE UNIX_TIMESTAMP() BETWEEN `event`.`start` AND `event`.`end`
            AND (`team`.`id` IN %s OR `team_subscription`.`team_id` IN %s)
        """  # Added placeholder for role filter later if needed

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching data

        # 1. Get subscription teams for teams owning the service, along with the teams that own the service
        cursor.execute(
            """SELECT `team`.`id` AS `team_id`, `team`.`override_phone_number`, `team`.`name` FROM `team_service`
                          JOIN `service` ON `service`.`id` = `team_service`.`service_id`
                          JOIN `team` ON `team`.`id` = `team_service`.`team_id`
                          WHERE `service`.`name` = %s""",
            (service,),  # Parameterize service name
        )
        team_service_data = cursor.fetchall()  # Fetch all results

        # Extract team IDs and override numbers
        team_ids = [row["team_id"] for row in team_service_data]
        team_override_numbers = {
            row["name"]: row["override_phone_number"]
            for row in team_service_data
        }

        # Check if any teams were found for the service
        if not team_ids:
            # If no teams, return an empty list immediately within the with block.
            # The context manager handles closing the connection.
            resp.text = json_dumps([])
            return  # Exit the function

        # 2. Build parameters for the main on-call query
        # The query uses team_ids twice for the two IN %s clauses
        query_params = [
            tuple(team_ids),
            tuple(team_ids),
        ]  # Pass team_ids as tuples for IN clauses

        # Handle optional role filter
        role_name = req.get_param("role")  # Get role from query parameters
        final_oncall_query = (
            get_oncall_query_template  # Start with the template
        )
        if role_name is not None:
            final_oncall_query += (
                " AND `role`.`name` = %s"  # Add role condition
            )
            query_params.append(role_name)  # Add role name parameter

        # 3. Execute the main on-call query
        cursor.execute(final_oncall_query, query_params)

        # Fetch all results for on-call events
        oncall_events_data = cursor.fetchall()

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # --- Post-processing logic outside the with block ---
    # This logic operates on the fetched oncall_events_data list.
    ret = {}
    for row in oncall_events_data:
        user = row["user"]
        # add data row into accumulator only if not already there
        if user not in ret:
            # Copy essential fields, excluding raw contact details
            user_summary = {
                k: v for k, v in row.items() if k not in ["mode", "destination"]
            }
            ret[user] = user_summary
            ret[user]["contacts"] = {}

        # Add contact details if present
        mode = row.get("mode")  # Use .get for safety
        dest = row.get("destination")  # Use .get for safety
        if mode is not None and dest is not None:
            ret[user]["contacts"][mode] = dest

    # Apply team override phone numbers if applicable
    final_data = list(ret.values())  # Convert dictionary values to a list

    for event in final_data:
        team_name = event.get("team")  # Get team name from the event data
        # Check if the event is for a primary role and if the team has an override number
        if team_name and event.get("role") == "primary":
            override_number = team_override_numbers.get(team_name)
            if override_number:
                # Ensure contacts dict exists before trying to update
                if "contacts" not in event:
                    event["contacts"] = {}
                event["contacts"]["call"] = override_number
                event["contacts"]["sms"] = override_number

    # Set the response text with the final processed data
    resp.text = json_dumps(final_data)
