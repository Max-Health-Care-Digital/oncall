# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time
from operator import itemgetter

from falcon import (  # HTTPNotFound used below, remove unused import from exception
    HTTPBadRequest,
    HTTPNotFound,
)
from ujson import dumps as json_dumps  # Use json_dumps

from ... import db


def on_get(req, resp, team, roster, role):
    """
    Get the current user on-call for a given service/role. Returns event start/end, contact info,
    and user name.
    """
    # Ensure start and end are provided and are integers
    start = req.get_param_as_int("start", required=True)
    end = req.get_param_as_int("end", required=True)

    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching results by name

        try:
            # 1. Get role ID
            cursor.execute(
                "SELECT id FROM role WHERE name = %s", (role,)
            )  # Parameterize role name
            role_row = cursor.fetchone()
            if not role_row:  # fetchone returns None if no rows found
                raise HTTPBadRequest(description=f"Invalid role name: '{role}'")
            role_id = role_row["id"]  # Use dictionary access

            # 2. Get team ID and roster ID
            cursor.execute(
                """SELECT `team`.`id` AS `team_id`, `roster`.`id` AS `roster_id` FROM `team` JOIN `roster` ON `roster`.`team_id` = `team`.`id`
                              WHERE `roster`.`name` = %s and `team`.`name` = %s""",
                (roster, team),  # Parameterize roster name and team name
            )
            team_roster_row = cursor.fetchone()
            if not team_roster_row:  # fetchone returns None if no rows found
                raise HTTPBadRequest(
                    description=f"Invalid team '{team}' or roster '{roster}' name"
                )
            team_id = team_roster_row["team_id"]  # Use dictionary access
            roster_id = team_roster_row["roster_id"]  # Use dictionary access

            # 3. Get roster size (number of in-rotation users)
            cursor.execute(
                "SELECT COUNT(*) FROM roster_user WHERE roster_id = %s AND `in_rotation` = 1",
                (roster_id,),  # Count only in_rotation users
            )
            roster_size_row = cursor.fetchone()
            # This fetch should not fail if the roster exists, but the count might be 0.
            # If the count is 0, there are no candidates anyway.
            # Raising HTTPNotFound seems odd here; returning an empty candidate list is more appropriate if roster is empty.
            # Keeping the original check for now, but noting it raises 404 even if roster exists but is empty.
            if (
                not roster_size_row or roster_size_row["COUNT(*)"] == 0
            ):  # Check count value
                raise HTTPNotFound(
                    description=f"Roster '{roster}' for team '{team}' has no in-rotation users or was not found (check team/roster names again?)"
                )  # Improved message
            roster_size = roster_size_row["COUNT(*)"]

            # Calculate length based on roster size
            # Assuming 604800 is 1 week in seconds (24*60*60*7)
            WEEK_IN_SECONDS = 604800
            length = WEEK_IN_SECONDS * roster_size

            # 4. Prepare data dictionary for parameterized queries
            data_params = {
                "team_id": team_id,
                "roster_id": roster_id,
                "role_id": role_id,
                "past": start - length,
                "start": start,
                "end": end,
                "future": start + length,
            }

            # 5. Get users busy during the requested override time range
            cursor.execute(
                """SELECT `user`.`name` FROM `event` JOIN `user` ON `event`.`user_id` = `user`.`id`
                              WHERE `team_id` = %(team_id)s AND %(start)s < `event`.`end` AND %(end)s > `event`.`start`""",
                data_params,  # Use dictionary parameters
            )
            busy_users = set(
                row["name"] for row in cursor
            )  # Use dictionary access for name

            # 6. Get availability scores for candidate users (complex query)
            cursor.execute(
                """SELECT * FROM
                                (SELECT `user`.`name` AS `user`, MAX(`event`.`start`) AS `before`
                                 FROM `roster_user` JOIN `user` ON `user`.`id` = `roster_user`.`user_id`
                                   AND roster_id = %(roster_id)s AND `roster_user`.`in_rotation` = 1
                                 LEFT JOIN `event` ON `event`.`user_id` = `user`.`id` AND `team_id` = %(team_id)s
                                   AND `role_id` = %(role_id)s AND `start` BETWEEN %(past)s AND %(start)s
                                 GROUP BY `user`.`name`) past
                              JOIN
                                (SELECT `user`.`name` AS `user`, MIN(`event`.`start`) AS `after`
                                 FROM `roster_user` JOIN `user` ON `user`.`id` = `roster_user`.`user_id`
                                   AND roster_id = %(roster_id)s AND `roster_user`.`in_rotation` = 1
                                 LEFT JOIN `event` ON `event`.`user_id` = `user`.`id` AND `team_id` = %(team_id)s
                                   AND `role_id` = %(role_id)s AND `start` BETWEEN %(start)s AND %(future)s
                                 GROUP BY `user`.`name`) future
                              USING (`user`)""",
                data_params,  # Use dictionary parameters
            )
            # Fetch results from the complex query
            availability_data = cursor.fetchall()

            # The connection and cursor will be automatically closed/released
            # when the 'with' block exits, even if an error occurs.
            # Explicit close calls are no longer needed.

        except Exception as e:  # Catch any exceptions during DB interaction
            # The with statement handles rollback automatically.
            print(
                f"Error during candidate selection for team={team}, roster={roster}, role={role}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

    # --- Post-processing logic outside the with block ---
    # This logic operates on busy_users and availability_data fetched inside.

    candidate = None
    max_score = -1
    ret = {}  # Dictionary to store scores

    # Find argmax(min(time between start and last event, time before start and next event))
    # If no next/last event exists, set value to infinity
    # This should maximize gaps between shifts
    for (
        row
    ) in availability_data:  # Iterate through the fetched availability data
        user = row["user"]  # Use dictionary access
        before = row.get(
            "before"
        )  # Use .get for safety, can be None if LEFT JOIN found no event
        after = row.get(
            "after"
        )  # Use .get for safety, can be None if LEFT JOIN found no event

        if user in busy_users:
            continue  # Skip users busy during the requested slot

        before_score = start - before if before is not None else float("inf")
        after_score = after - start if after is not None else float("inf")
        score = min(before_score, after_score)

        ret[user] = score if score != float("inf") else "infinity"

        if score > max_score:
            candidate = user
            max_score = score

    # Set the response text with the best candidate and all scores
    resp.text = json_dumps({"user": candidate, "data": ret})
