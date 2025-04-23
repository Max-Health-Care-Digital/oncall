# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from falcon import HTTPNotFound, HTTPBadRequest, HTTPInternalServerError, HTTPError

from oncall.bin.scheduler import load_scheduler

from ... import db
from ...auth import check_team_auth, login_required
from ...utils import load_json_body
from .schedules import get_schedules

from json import dumps as json_dumps
import logging

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@login_required
def on_post(req, resp, schedule_id):
    """
    Manually trigger scheduler population for a given schedule starting from a specific time.
    Expects JSON body: {'start': unix_timestamp}
    """
    try:
        schedule_id_int = int(schedule_id)
        data = load_json_body(req)
        start_time = data.get("start")
        if start_time is None:
            raise HTTPBadRequest(
                "Missing Parameter", 'Request body must contain "start" timestamp.'
            )
        start_time = int(start_time) # Ensure start_time is int

    except ValueError:
        raise HTTPBadRequest(
            "Invalid Parameter", "Schedule ID and start time must be integers."
        )
    except Exception as e:
        # Catch potential errors from load_json_body or int conversion
        raise HTTPBadRequest(
            "Invalid Request", f"Failed to process request body: {e}"
        )

    try:
        # Use 'with' statement for connection and transaction management
        with db.connect() as connection:
            cursor = None # Initialize cursor
            try:
                cursor = connection.cursor(db.DictCursor)

                # 1. Fetch schedule and scheduler name
                # Use get_schedules which handles its own DB interaction or uses provided dbinfo
                # We need the scheduler name and team name for auth
                # Fetching schedule details first to perform auth check early
                schedules_list = get_schedules(
                    {"id": schedule_id_int}, dbinfo=(connection, cursor)
                )
                if not schedules_list:
                    raise HTTPNotFound(description=f"Schedule {schedule_id_int} not found.")

                schedule = schedules_list[0] # get_schedules returns a list

                # 2. Authorization Check
                check_team_auth(schedule.get("team"), req) # Check auth using team name from schedule

                # 3. Load Scheduler Module
                scheduler_info = schedule.get("scheduler", {})
                scheduler_name = scheduler_info.get("name") if isinstance(scheduler_info, dict) else None
                if not scheduler_name:
                     raise HTTPInternalServerError(title="Configuration Error", description=f"Schedule {schedule_id_int} has no associated scheduler.")

                try:
                    scheduler = load_scheduler(scheduler_name)
                except (ImportError, AttributeError) as e:
                     logger.error(f"Failed to load scheduler '{scheduler_name}': {e}", exc_info=True)
                     raise HTTPInternalServerError(title="Scheduler Load Error", description=f"Failed to load scheduler module '{scheduler_name}'.")

                # 4. Call scheduler's populate method
                # The populate method should handle its own commit/rollback within the transaction
                scheduler.populate(schedule, start_time, (connection, cursor))

                # populate method handles commit, no explicit commit here.

            except HTTPError:
                # Re-raise HTTP errors (like HTTPNotFound, HTTPBadRequest from populate)
                raise
            except Exception as e:
                # Log unexpected errors during the population process
                logger.exception(f"Error populating schedule {schedule_id_int}: {e}")
                # Rollback is handled by 'with' block
                raise HTTPInternalServerError(title="Population Error", description=f"An unexpected error occurred during schedule population: {e}")
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception as cur_e:
                        logger.warning(f"Error closing cursor: {cur_e}")

        # Connection automatically closed/returned by 'with'

    except db.Error as e:
        logger.exception(f"Database connection error during populate for schedule {schedule_id_int}: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except Exception as e:
        # Catch errors outside the 'with db.connect()' block if any
        logger.exception(f"Unexpected error in populate endpoint for schedule {schedule_id_int}: {e}")
        raise HTTPInternalServerError(description=f"An unexpected error occurred: {e}")

    # If successful, return 200 OK (or 204 No Content)
    resp.status_code = 200 # Or falcon.HTTP_200
    resp.text = json_dumps({"message": f"Schedule {schedule_id_int} population triggered successfully."})
