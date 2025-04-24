# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from json import dumps as json_dumps

from falcon import (
    HTTP_200,
    HTTPBadRequest,
    HTTPError,
    HTTPForbidden,
    HTTPInternalServerError,
    HTTPNotFound,
)

from ... import db
from ...auth import check_team_auth, login_required
from ...utils import load_json_body
from .schedules import (
    get_schedules,
    insert_schedule_events,
    validate_simple_schedule,
)

columns = {
    "role": "`role_id`=(SELECT `id` FROM `role` WHERE `name`=%(role)s)",
    "team": "`team_id`=(SELECT `id` FROM `team` WHERE `name`=%(team)s)",
    "roster": "`roster_id`=(SELECT `roster`.`id` FROM `roster` JOIN `team` ON `roster`.`team_id` = `team`.`id` "
    "WHERE `roster`.`name`=%(roster)s AND `team`.`name`=%(team)s)",
    "auto_populate_threshold": "`auto_populate_threshold`=%(auto_populate_threshold)s",
    "advanced_mode": "`advanced_mode` = %(advanced_mode)s",
    "scheduler": "`scheduler_id`=(SELECT `id` FROM `scheduler` WHERE `name` = %(scheduler)s)",
}


# Refactored verify_auth: No longer manages connection/cursor
def verify_auth(req, schedule_id, cursor):
    """
    Verifies that the requesting user has auth for the team owning the schedule.
    Uses the provided cursor. Raises HTTPNotFound or HTTPForbidden on failure.

    Args:
        req: The Falcon request object.
        schedule_id: The ID of the schedule to check.
        cursor: An active database cursor.

    Returns:
        str: The name of the team owning the schedule if found and auth succeeds.

    Raises:
        HTTPNotFound: If the schedule_id does not exist.
        HTTPForbidden: If the user is not authorized for the schedule's team.
    """
    team_query = (
        "SELECT `team`.`name` FROM `schedule` JOIN `team` "
        "ON `schedule`.`team_id` = `team`.`id` WHERE `schedule`.`id` = %s"
    )
    cursor.execute(team_query, (schedule_id,))  # Pass schedule_id as tuple
    result = cursor.fetchone()
    if not result:
        # Let the caller handle connection/cursor closure
        raise HTTPNotFound(
            description=f"Schedule with ID {schedule_id} not found."
        )

    team_name = result[0]
    # check_team_auth will raise HTTPForbidden if auth fails
    check_team_auth(team_name, req)
    # Return team_name in case the caller needs it (optional)
    return team_name


# --- on_get remains unchanged, assuming get_schedules is refactored elsewhere ---
def on_get(req, resp, schedule_id):
    """
    Get schedule information. Detailed information on schedule parameters is provided in the
    POST method for /api/v0/team/{team_name}/rosters/{roster_name}/schedules.

    **Example request**:

    .. sourcecode:: http

        GET /api/v0/schedules/1234  HTTP/1.1
        Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

            {
                "advanced_mode": 1,
                "auto_populate_threshold": 30,
                "events": [
                    {
                        "duration": 259200,
                        "start": 0
                    }
                ],
                "id": 1234,
                "role": "primary",
                "role_id": 1,
                "roster": "roster-foo",
                "roster_id": 2922,
                "team": "asdf",
                "team_id": 2121,
                "timezone": "US/Pacific"
            }
    """
    resp.text = json_dumps(
        get_schedules(
            {"id": schedule_id}, fields=req.get_param_as_list("fields")
        )[0]
    )


@login_required
def on_put(req, resp, schedule_id):
    """
    Update a schedule. Allows editing of role, team, roster, auto_populate_threshold,
    events, and advanced_mode.Only allowed for team admins. Note that simple mode
    schedules must conform to simple schedule restrictions (described in documentation
    for the /api/v0/team/{team_name}/rosters/{roster_name}/schedules GET endpoint).
    This is checked on both "events" and "advanced_mode" edits.

    **Example request:**

    .. sourcecode:: http

        PUT /api/v0/schedules/1234 HTTP/1.1
        Content-Type: application/json

        {
            "role": "primary",
            "team": "team-bar",
            "roster": "roster-bar",
            "auto_populate_threshold": 28,
            "events":
                [
                    {
                        "start": 0,
                        "duration": 100
                    }
                ]
            "advanced_mode": 1
        }
    """
    try:
        # Ensure schedule_id is an integer early on
        schedule_id_int = int(schedule_id)
        data = load_json_body(req)
    except ValueError:
        raise HTTPBadRequest("Invalid ID", "Schedule ID must be an integer.")
    except Exception as e:
        raise HTTPBadRequest(
            "Invalid Request", f"Failed to process request body: {e}"
        )

    # Prepare data for update
    events = data.pop("events", None)
    scheduler = data.pop("scheduler", None)
    update_data = {}
    if scheduler:
        # Assuming scheduler value should be the name string for the DB query
        update_data["scheduler"] = scheduler.get("name")

    # Filter data based on allowed columns and prepare for named placeholders
    for k, v in data.items():
        if k in columns:
            update_data[k] = v

    if not update_data and not events and not scheduler:
        raise HTTPBadRequest(
            "No changes", "No valid fields provided for update."
        )

    if "roster" in update_data and "team" not in update_data:
        # If roster is updated, team context might be needed depending on `columns` definition
        # If 'team' key isn't naturally in update_data, this check prevents errors
        # Consider fetching current team if only roster is changed? Or require both?
        # Assuming `columns['roster']` requires `%(team)s`.
        raise HTTPBadRequest(
            "Invalid edit", "Team name must be specified when updating roster."
        )

    # Build the SET part of the query using named placeholders from the `columns` dict
    set_clauses = []
    query_params = {}
    for key, value in update_data.items():
        if key in columns:
            # columns[key] is like '`col_name`=%(key_name)s'
            set_clauses.append(columns[key])
            query_params[key] = value  # Add value to dictionary for execution

    # Add schedule ID to parameters for the WHERE clause
    query_params["schedule_id"] = schedule_id_int

    try:
        with db.connect() as connection:
            cursor = connection.cursor()
            try:
                # Verify auth using the refactored helper within the transaction
                verify_auth(req, schedule_id_int, cursor)

                # --- Validation for simple schedule ---
                current_events = None
                if events:
                    # Validate new events if provided
                    is_simple_schedule = validate_simple_schedule(events)
                else:
                    # Fetch existing events to validate if needed later
                    cursor.execute(
                        "SELECT start, duration FROM schedule_event WHERE schedule_id = %s ORDER BY start ASC",
                        (schedule_id_int,),
                    )
                    current_events = cursor.fetchall()
                    # Format as list of dicts for validation function
                    existing_event_dicts = [
                        {"start": ev[0], "duration": ev[1]}
                        for ev in current_events
                    ]
                    is_simple_schedule = validate_simple_schedule(
                        existing_event_dicts
                    )

                # Determine target advanced_mode (new value or existing)
                target_advanced_mode = update_data.get("advanced_mode")
                if target_advanced_mode is None:
                    cursor.execute(
                        "SELECT advanced_mode FROM schedule WHERE id = %s",
                        (schedule_id_int,),
                    )
                    fetch_result = cursor.fetchone()
                    if fetch_result:
                        target_advanced_mode = fetch_result[0]
                    else:
                        # Should have been caught by verify_auth, but belt-and-suspenders
                        raise HTTPNotFound(
                            description=f"Schedule {schedule_id_int} vanished."
                        )
                else:
                    # Ensure boolean/int conversion if needed
                    target_advanced_mode = (
                        1 if bool(target_advanced_mode) else 0
                    )
                    update_data["advanced_mode"] = (
                        target_advanced_mode  # Ensure update_data has correct type
                    )

                # Check consistency: If target is simple mode, events must conform
                if not target_advanced_mode and not is_simple_schedule:
                    raise HTTPBadRequest(
                        "Invalid edit",
                        "Schedule events are not valid for simple mode.",
                    )
                # --- End Validation ---

                # --- Apply Updates ---
                if set_clauses:
                    update_query = "UPDATE `schedule` SET {0} WHERE `id`=%(schedule_id)s".format(
                        ", ".join(set_clauses)
                    )
                    cursor.execute(update_query, query_params)

                if events is not None:  # Use is not None to allow empty list []
                    cursor.execute(
                        "DELETE FROM `schedule_event` WHERE `schedule_id` = %s",
                        (schedule_id_int,),
                    )
                    if events:  # Only insert if new events list is not empty
                        insert_schedule_events(schedule_id_int, events, cursor)

                if (
                    scheduler
                    and scheduler.get("name") == "round-robin"
                    and "data" in scheduler
                ):
                    # Use executemany for efficiency and safety
                    order_params = [
                        (schedule_id_int, name, idx)
                        for idx, name in enumerate(scheduler.get("data", []))
                    ]
                    if order_params:  # Only run if there are users
                        cursor.execute(
                            "DELETE FROM `schedule_order` WHERE `schedule_id` = %s",
                            (schedule_id_int,),
                        )
                        cursor.executemany(
                            """INSERT INTO `schedule_order` (`schedule_id`, `user_id`, `priority`)
                                VALUES (%s, (SELECT `id` FROM `user` WHERE `name` = %s LIMIT 1), %s)""",
                            order_params,
                        )

                # Commit the transaction
                connection.commit()

            except (HTTPBadRequest, HTTPNotFound, HTTPForbidden) as e:
                connection.rollback()  # Rollback on known HTTP errors during transaction
                raise e  # Re-raise the specific HTTP error
            except Exception as e:
                connection.rollback()  # Rollback on any other error
                print(f"Error during schedule update transaction: {e}")  # Log
                # Check for integrity errors specifically?
                # if isinstance(e, db.IntegrityError): raise HTTPBadRequest(...)
                raise HTTPInternalServerError(
                    description=f"Failed to update schedule: {e}"
                ) from e

    except db.Error as e:
        print(f"Database connection error in on_put schedule: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise HTTP errors from initial checks or caught above
        raise e
    except Exception as e:
        print(f"Unexpected error in on_put schedule: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # If successful, return 200 OK (no explicit body needed unless specified)
    resp.status = HTTP_200  # Standard for successful PUT update


@login_required
def on_delete(req, resp, schedule_id):
    """
    Delete a schedule by id. Only allowed for team admins.

    **Example request:**

    .. sourcecode:: http

        DELETE /api/v0/schedules/1234 HTTP/1.1

    :statuscode 200: Successful delete
    :statuscode 404: Schedule not found
    """
    try:
        schedule_id_int = int(schedule_id)
    except ValueError:
        raise HTTPBadRequest("Invalid ID", "Schedule ID must be an integer.")

    deleted_count = 0  # Initialize delete count

    try:
        with db.connect() as connection:
            cursor = connection.cursor()
            try:
                # Verify auth using the refactored helper
                verify_auth(req, schedule_id_int, cursor)

                # Execute delete (implicitly deletes related schedule_event/order via FK constraints usually)
                # If no FK constraints, delete from child tables first (schedule_event, schedule_order)
                # Assuming FKs handle cascading deletes or are handled elsewhere:
                cursor.execute(
                    "DELETE FROM `schedule` WHERE `id`=%s", (schedule_id_int,)
                )
                deleted_count = cursor.rowcount  # Store rowcount before commit

                # Commit the transaction
                connection.commit()

            except (HTTPNotFound, HTTPForbidden) as e:
                # Re-raise specific errors from verify_auth
                connection.rollback()
                raise e
            except Exception as e:
                connection.rollback()  # Rollback on other errors
                print(f"Error during schedule delete transaction: {e}")  # Log
                raise HTTPInternalServerError(
                    description=f"Failed to delete schedule: {e}"
                ) from e

    except db.Error as e:
        print(f"Database connection error in on_delete schedule: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise other HTTP errors (like auth or bad request)
        raise e
    except Exception as e:
        print(f"Unexpected error in on_delete schedule: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # Check if anything was actually deleted after successful commit
    if deleted_count == 0:
        # This means verify_auth succeeded but the schedule was gone before DELETE command ran,
        # or the ID was invalid but somehow passed verify_auth (less likely).
        # Raising 404 here is appropriate.
        raise HTTPNotFound(
            description=f"Schedule with ID {schedule_id_int} not found for deletion."
        )

    # If deletion occurred, return 200 OK
    resp.status = HTTP_200  # Standard for successful DELETE
