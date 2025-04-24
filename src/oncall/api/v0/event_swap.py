# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time
from operator import itemgetter

from falcon import HTTP_200  # Added for successful response status
from falcon import HTTPBadRequest  # HTTPNotFound imported but not used?
from falcon import HTTPError, HTTPNotFound

from ... import constants, db
from ...auth import check_calendar_auth_by_id, login_required
from ...constants import EVENT_SWAPPED
from ...utils import create_audit  # Assuming create_audit takes a cursor
from ...utils import (  # Assuming create_notification takes a cursor
    create_notification,
    load_json_body,
)


@login_required
def on_post(req, resp):
    """
    Swap events. Takes an object specifying the 2 events to be swapped. Swap can
    take either single events or event sets, depending on the value of the
    "linked" attribute. If "linked" is True, the API interprets the "id"
    attribute as a link_id. Otherwise, it's assumed to be an event_id. Note
    that this allows swapping a single event with a linked event.

    **Example request**:

    .. sourcecode:: http

        POST api/v0/events/swap   HTTP/1.1
        Content-Type: application/json

        {
            "events":
            [
                {
                    "id": 1,
                    "linked": false
                },
                {
                    "id": "da515a45e2b2467bbdc9ea3bc7826d36",
                    "linked": true
                }
            ]
        }

    :statuscode 200: Successful swap
    :statuscode 400: Validation checks failed
    """
    data = load_json_body(req)

    # Basic validation before DB interaction: check for exactly 2 events
    try:
        ev_0, ev_1 = data.get("events", [])  # Use .get with default empty list
        if len(data.get("events", [])) != 2:  # Explicitly check length
            raise ValueError(
                "Must provide exactly 2 events"
            )  # Raise ValueError first
    except (
        TypeError,
        ValueError,
    ) as e:  # Catch if data.get("events") is not iterable or has wrong length
        raise HTTPBadRequest(
            "Invalid event swap request",
            f"Must provide a list of exactly 2 events: {e}",
        )

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching event data

        try:
            # Accumulate event info for each link/event id
            fetched_events_lists = [
                None,
                None,
            ]  # Will store list of events for ev_0 and ev_1

            for i, ev in enumerate([ev_0, ev_1]):
                # Validate event structure
                if not isinstance(ev, dict):
                    raise HTTPBadRequest(
                        "Invalid event swap request",
                        f"Event at index {i} is not an object",
                    )

                event_id_or_link_id = ev.get("id")
                is_linked = ev.get("linked", False)  # Default linked to False

                if not event_id_or_link_id:
                    raise HTTPBadRequest(
                        "Invalid event swap request",
                        f"Event at index {i} has an invalid or missing 'id'",
                    )
                # Ensure id is int if not linked, or str if linked (UUID)
                if not is_linked and not isinstance(
                    event_id_or_link_id, (int, str)
                ):
                    raise HTTPBadRequest(
                        "Invalid event swap request",
                        f"Event ID at index {i} must be an integer or string",
                    )
                if is_linked and not isinstance(event_id_or_link_id, str):
                    raise HTTPBadRequest(
                        "Invalid event swap request",
                        f"Link ID at index {i} must be a string",
                    )

                # Fetch events based on id or link_id
                if is_linked:
                    # Fetch events by link_id
                    cursor.execute(
                        """SELECT `id`, `start`, `end`, `team_id`, `user_id`, `role_id`,
                                     `link_id` FROM `event` WHERE `link_id` = %s""",
                        (
                            event_id_or_link_id,
                        ),  # Parameterize link_id as a tuple
                    )
                else:
                    # Fetch events by event_id
                    # Ensure event_id is an integer if not linked
                    try:
                        event_id_int = int(event_id_or_link_id)
                    except (ValueError, TypeError):
                        raise HTTPBadRequest(
                            "Invalid event swap request",
                            f"Event ID at index {i} must be an integer",
                        )

                    cursor.execute(
                        """SELECT `id`, `start`, `end`, `team_id`, `user_id`, `role_id`,
                                     `link_id` FROM `event` WHERE `id` = %s""",
                        (event_id_int,),  # Parameterize event_id as a tuple
                    )

                fetched_list = (
                    cursor.fetchall()
                )  # Fetch all results for this id/link_id

                if not fetched_list:
                    # If no events found for the id/link_id
                    raise HTTPNotFound(
                        description=f"Event or linked events with ID '{event_id_or_link_id}' not found"
                    )

                fetched_events_lists[i] = (
                    fetched_list  # Store the list of fetched events
                )

            # Unpack the fetched lists
            events_0, events_1 = fetched_events_lists
            # Concatenate all fetched events for validation
            all_fetched_events = events_0 + events_1

            # 3. Perform Validation checks on fetched events
            now = time.time()
            if any(
                ev.get("start", 0) < now - constants.GRACE_PERIOD
                for ev in all_fetched_events
            ):  # Use .get with default
                raise HTTPBadRequest(
                    "Invalid event swap request",
                    "Cannot swap events that started in the past",  # Adjusted message
                )

            # Check if all swapped events belong to the same team
            if (
                len(
                    set(
                        ev.get("team_id")
                        for ev in all_fetched_events
                        if ev.get("team_id") is not None
                    )
                )
                > 1
            ):  # Check non-None team_ids
                raise HTTPBadRequest(
                    "Event swap not allowed",
                    "Swapped events must come from the same team",
                )
            # Get the single team_id (assuming check passed)
            team_id = all_fetched_events[0].get(
                "team_id"
            )  # Get team_id from the first event

            # Check calendar auth for the team
            check_calendar_auth_by_id(team_id, req)

            # Check if all linked events *within each group* have the same original user
            for ev_list in [events_0, events_1]:
                # Only check if the group is not empty and has more than one event
                if ev_list and len(ev_list) > 1:
                    if (
                        len(
                            set(
                                ev.get("user_id")
                                for ev in ev_list
                                if ev.get("user_id") is not None
                            )
                        )
                        != 1
                    ):  # Check non-None user_ids
                        # This error message might need clarification - it applies *per list* (events_0 or events_1)
                        raise HTTPBadRequest(
                            "Invalid event swap request",
                            "All linked events within each swap group must have the same user",
                        )

            # Extract original user IDs for swapping (from the first event of each list)
            user_0 = events_0[0].get("user_id")  # Use .get
            user_1 = events_1[0].get("user_id")  # Use .get
            if (
                user_0 is None or user_1 is None
            ):  # Should not happen if events were found and have user_id
                raise HTTPError(
                    "500 Internal Server Error",
                    "Data Error",
                    "Could not get user ID from fetched event data",
                )

            # Find the first event by start time in each list for notification context
            first_event_0 = min(
                events_0, key=lambda ev: ev.get("start", float("inf"))
            )  # Use .get with default for safety
            first_event_1 = min(
                events_1, key=lambda ev: ev.get("start", float("inf"))
            )  # Use .get with default for safety

            # 4. Execute Update Queries to swap users
            # All updates are within the same transaction

            # Update events in events_0: set user_id to user_1, break link if not originally linked
            ids_0 = [
                e0.get("id") for e0 in events_0 if e0.get("id") is not None
            ]  # Get non-None IDs
            if ids_0:  # Only execute if there are IDs
                update_query_0 = "UPDATE `event` SET `user_id` = %s"
                params_0 = [user_1]
                # Break link if the first event in the list was NOT linked
                if not events_0[0].get(
                    "linked"
                ):  # Check the 'linked' flag from the request input
                    update_query_0 += ", `link_id` = NULL"
                update_query_0 += " WHERE `id` IN %s"
                params_0.append(tuple(ids_0))  # Add tuple of IDs for IN clause

                cursor.execute(update_query_0, params_0)

            # Update events in events_1: set user_id to user_0, break link if not originally linked
            ids_1 = [
                e1.get("id") for e1 in events_1 if e1.get("id") is not None
            ]  # Get non-None IDs
            if ids_1:  # Only execute if there are IDs
                update_query_1 = "UPDATE `event` SET `user_id` = %s"
                params_1 = [user_0]
                # Break link if the second event in the request (ev_1) was NOT linked
                # Need to use the 'linked' flag from the original request input (ev_1), not the fetched data
                if not ev_1.get(
                    "linked"
                ):  # Check the 'linked' flag from request input ev_1
                    update_query_1 += ", `link_id` = NULL"
                update_query_1 += " WHERE `id` IN %s"
                params_1.append(tuple(ids_1))  # Add tuple of IDs for IN clause

                cursor.execute(update_query_1, params_1)

            # 5. Fetch user full names, team name for notification context
            # Need user_0 and user_1 IDs
            cursor.execute(
                "SELECT id, full_name FROM user WHERE id IN %s",
                (tuple([user_0, user_1]),),  # Parameterize user IDs
            )
            full_names = {
                row["id"]: row["full_name"] for row in cursor.fetchall()
            }

            # Need team name (can get from team_id)
            cursor.execute(
                "SELECT name FROM team WHERE id = %s",
                (team_id,),  # Parameterize team_id
            )
            team_row = cursor.fetchone()
            team_name = team_row["name"] if team_row else "N/A"  # Get team name

            # 6. Create notification context
            notification_context = {
                "full_name_0": full_names.get(user_0),  # Full name of user_0
                "full_name_1": full_names.get(user_1),  # Full name of user_1
                "team": team_name,  # Team name
                "swap_start_0": first_event_0.get(
                    "start"
                ),  # Start time of first event in list 0
                "swap_start_1": first_event_1.get(
                    "start"
                ),  # Start time of first event in list 1
                # Original code used role_id from events_0[0] and events_1[0]
                "role_0": events_0[0].get("role_id"),
                "role_1": events_1[0].get("role_id"),
            }

            # 7. Create notification
            # Notification needs team_id, roles affected, users affected, and start times
            affected_user_ids = [
                user_0,
                user_1,
            ]  # Users whose schedules changed
            affected_role_ids = {
                events_0[0].get("role_id"),
                events_1[0].get("role_id"),
            }  # Roles involved

            create_notification(
                notification_context,
                team_id,  # Team ID
                list(affected_role_ids),  # Roles affected (as a list)
                EVENT_SWAPPED,
                affected_user_ids,  # Users affected
                cursor,  # Pass the cursor
                start_time_0=first_event_0.get(
                    "start"
                ),  # Start time of first event in list 0
                start_time_1=first_event_1.get(
                    "start"
                ),  # Start time of first event in list 1
            )

            # 8. Create audit trail entry
            create_audit(
                {"request_body": data, "events_swapped": (events_0, events_1)},
                team_name,  # Team name for audit
                EVENT_SWAPPED,
                req,
                cursor,  # Pass the cursor
            )

            # 9. Commit the transaction if all steps in the try block succeed
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

        except HTTPError:  # Catch HTTPError raised within the try block
            raise  # Re-raise HTTPError for Falcon to handle
        except (
            db.IntegrityError
        ) as e:  # Catch IntegrityError from DB operations
            # The 'with' statement's __exit__ will automatically call rollback.
            err_msg = str(e.args[1])
            # Add specific IntegrityError messages if applicable (e.g., non-existent user during update, though SELECT should catch this)
            # The update queries use user_id (%s) and id/link_id (IN %s), so integrity errors related to these should be caught.
            err_msg = f"Database Integrity Error during swap: {err_msg}"

            raise HTTPError(
                "422 Unprocessable Entity", "IntegrityError", err_msg
            ) from e
        except (
            Exception
        ) as e:  # Catch any other unexpected exceptions during the transaction
            # The with statement handles rollback automatically.
            print(
                f"Error during event swap transaction: {e}"
            )  # Replace with logging
            raise  # Re-raise the exception

        # Do not need finally block; rely on the 'with' statement.

    resp.status = HTTP_200  # Keep original 200 status for success
