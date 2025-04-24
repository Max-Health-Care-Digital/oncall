# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time
import uuid  # Need to import uuid if gen_link_id uses it

from falcon import HTTP_201, HTTPBadRequest, HTTPError
from ujson import dumps as json_dumps

from ... import constants, db
from ...auth import check_calendar_auth, login_required
from ...utils import gen_link_id, load_json_body, user_in_team_by_name


@login_required
def on_post(req, resp):
    """
    Endpoint for creating linked events. Responds with event ids for created events.
    Linked events can be swapped in a group, and users are reminded only on the first event of a
    linked series. Linked events have a link_id attribute containing a uuid. All events
    with an equivalent link_id are considered "linked together" in a single set. Editing any single event
    in the set will break the link for that event, clearing the link_id field. Otherwise, linked events behave
    the same as any non-linked event.

    **Example request:**

    .. sourcecode:: http



        POST /api/v0/events/link HTTP/1.1
        Content-Type: application/json

        [
            {
                "start": 1493667700,
                "end": 149368700,
                "user": "jdoe",
                "team": "team-foo",
                "role": "primary",
            },
            {
                "start": 1493677700,
                "end": 149387700,
                "user": "jdoe",
                "team": "team-foo",
                "role": "primary",
            }
        ]

    **Example response:**

    .. sourcecode:: http

        HTTP/1.1 201 Created
        Content-Type: application/json

        {
            "link_id": "123456789abcdef0123456789abcdef0",
            "event_ids": [1, 2]
        }

    :statuscode 201: Event created
    :statuscode 400: Event validation checks failed
    :statuscode 422: Event creation failed: nonexistent role/event/team
    """
    events_list = load_json_body(req)  # Renamed variable
    if not isinstance(events_list, list):
        raise HTTPBadRequest(
            "Invalid argument", "events argument needs to be a list"
        )
    if not events_list:
        raise HTTPBadRequest("Invalid argument", "events list cannot be empty")

    # Basic validation before DB interaction: check first event for team
    first_event = events_list[0]
    team_name = first_event.get("team")
    if not team_name:
        raise HTTPBadRequest(
            "Invalid argument", "First event missing team attribute"
        )

    # Check calendar auth for the team
    check_calendar_auth(team_name, req)

    # Generate a single link_id for all events
    link_id = gen_link_id()

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()  # Use standard cursor

        try:
            # 1. Get team ID and validate team existence
            cursor.execute(
                "SELECT `id` FROM `team` WHERE `name`=%s", (team_name,)
            )  # Parameterize team name
            team_row = cursor.fetchone()
            if not team_row:
                # Raise HTTPBadRequest within the with block
                raise HTTPBadRequest(
                    "Invalid event", f"Invalid team name: {team_name}"
                )
            team_id = team_row[0]

            # 2. Prepare data and validate each event in the list
            event_values_for_executemany = []  # List of tuples for executemany
            now = time.time()  # Get current time once

            # Define the INSERT query template with %s placeholders for executemany
            # The order of columns and values placeholders MUST match the order of items in the tuple
            insert_query_template = """
                INSERT INTO `event`
                (`start`, `end`, `user_id`, `team_id`, `role_id`, `link_id`, `note`)
                VALUES (
                    %s, -- start
                    %s, -- end
                    (SELECT `id` FROM `user` WHERE `name`=%s), -- user name for subquery
                    %s, -- team_id
                    (SELECT `id` FROM `role` WHERE `name`=%s), -- role name for subquery
                    %s, -- link_id
                    %s  -- note
                )
            """  # This template requires 7 parameters per row

            for (
                ev
            ) in (
                events_list
            ):  # Iterate through the list of events from request body
                # Validate individual event fields and values
                # Ensure required fields are present in each event dict
                required_event_fields = {"start", "end", "user", "role"}
                if not required_event_fields.issubset(ev.keys()):
                    missing = required_event_fields - ev.keys()
                    raise HTTPBadRequest(
                        "Invalid event",
                        f"Event missing required parameters: {', '.join(missing)}",
                    )

                # Validate timestamps
                try:
                    ev_start = int(ev["start"])
                    ev_end = int(ev["end"])
                except (ValueError, TypeError):
                    raise HTTPBadRequest(
                        "Invalid event",
                        "Event start and end timestamps must be integers",
                    )

                if ev_start < now - constants.GRACE_PERIOD:
                    raise HTTPBadRequest(
                        "Invalid event",
                        "Creating events in the past not allowed",
                    )
                if ev_start >= ev_end:
                    raise HTTPBadRequest(
                        "Invalid event", "Event must start before it ends"
                    )

                # Validate team consistency
                ev_team = ev.get("team")  # Use .get
                if not ev_team:
                    raise HTTPBadRequest(
                        "Invalid event", "Missing team for an event"
                    )
                if team_name != ev_team:
                    raise HTTPBadRequest(
                        "Invalid event",
                        "Events can only be submitted to one team",
                    )

                # Validate user membership in the team using the current cursor
                # Assuming user_in_team_by_name takes a cursor
                if not user_in_team_by_name(cursor, ev["user"], team_name):
                    raise HTTPBadRequest(
                        "Invalid event",
                        f"User '{ev['user']}' must be part of the team '{team_name}'",
                    )

                # Validate note field if present
                ev_note = ev.get("note")
                if ev_note is not None and not isinstance(ev_note, str):
                    raise HTTPBadRequest(
                        "Invalid event", "Event note must be a string or null"
                    )

                # *** FIX: Prepare the tuple for executemany to match the query template ***
                # The tuple needs 7 items corresponding to the 7 placeholders (%s) in the template
                event_values_for_executemany.append(
                    (
                        ev_start,  # 1st %s: start time
                        ev_end,  # 2nd %s: end time
                        ev["user"],  # 3rd %s: user name for SELECT subquery
                        team_id,  # 4th %s: team_id (already fetched ID)
                        ev["role"],  # 5th %s: role name for SELECT subquery
                        link_id,  # 6th %s: link_id
                        ev_note,  # 7th %s: note (or None)
                    )
                )

            # 3. Execute batch insert using executemany
            # *** FIX: Use the query template and the list of tuples ***
            if (
                event_values_for_executemany
            ):  # Only execute if there are events to insert
                cursor.executemany(
                    insert_query_template, event_values_for_executemany
                )

            # 4. Commit the transaction if all inserts succeed
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

            # 5. Fetch the IDs of the newly created events using the link_id
            cursor.execute(
                "SELECT `id` FROM `event` WHERE `link_id`=%s ORDER BY `start`",
                (link_id,),  # Parameterize link_id
            )
            new_event_ids = [row[0] for row in cursor]  # Fetch new event IDs

        except db.IntegrityError as e:
            # The 'with' statement's __exit__ will automatically call rollback.
            err_msg = str(e.args[1])
            # Check for specific IntegrityError messages (likely from subqueries failing or unique constraints)
            if "Column 'role_id' cannot be null" in err_msg:
                # Role name in an event didn't resolve to an ID
                # Try to identify which role name caused it if possible, or use a generic message
                # The error message might contain clues from the query or parameters.
                # Example: if the error mentions a specific role name value that was attempted.
                # Without specific DB driver error codes/messages, generic messages based on common causes are safer.
                # Assume the error occurred because a role name didn't exist.
                err_msg = (
                    f"One or more role names in the events were not found."
                )
            elif "Column 'user_id' cannot be null" in err_msg:
                # User name in an event didn't resolve to an ID
                err_msg = (
                    f"One or more user names in the events were not found."
                )
            elif "Column 'team_id' cannot be null" in err_msg:
                # Team name in an event didn't resolve to an ID (should be caught by initial check, but defensive)
                err_msg = f"One or more team names in the events were not found. (Integrity Error)"
            # Add other potential IntegrityError checks if applicable (e.g., unique constraints if link_id+start+end+user+role should be unique)
            else:
                # Generic fallback for other integrity errors
                err_msg = (
                    f"Database Integrity Error during event creation: {err_msg}"
                )

            # Re-raise the exception after formatting the error message
            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e
        except (
            Exception
        ) as e:  # Catch any other unexpected exceptions during the transaction
            # The with statement handles rollback automatically.
            print(
                f"Error during linked event creation for link ID {link_id}: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_201
    # Respond with the generated link_id and the IDs of the new events
    resp.text = json_dumps({"link_id": link_id, "event_ids": new_event_ids})
