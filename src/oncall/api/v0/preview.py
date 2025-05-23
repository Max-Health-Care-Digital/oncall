# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import logging
import operator
import time  # Import time for potential use

from falcon import HTTP_200  # Import HTTP_200
from falcon import (
    HTTPBadRequest,
    HTTPError,
    HTTPInternalServerError,
    HTTPNotFound,
)
from ujson import dumps as json_dumps  # Import json_dumps

# Assuming load_scheduler is correctly imported
try:
    from oncall.bin.scheduler import load_scheduler
except ImportError:
    # Provide a fallback or raise a more specific error if scheduler loading is critical
    def load_scheduler(name):
        raise ImportError(
            f"Could not import load_scheduler from oncall.bin.scheduler. Scheduler '{name}' cannot be loaded."
        )


from ... import db
from .schedules import get_schedules

# Define constants for temporary table name
TEMP_EVENT_TABLE = "temp_event_preview"  # Use a more specific name perhaps
logger = logging.getLogger(__name__)  # Setup logger


def on_get(req, resp, schedule_id):
    """
    Preview the events that would be generated by a schedule population.
    """
    try:
        schedule_id_int = int(schedule_id)
        # Get start time from query param, default to now if not provided
        start_time_param = req.get_param_as_int("start")
        start_time = (
            start_time_param
            if start_time_param is not None
            else int(time.time())
        )

    except ValueError:
        raise HTTPBadRequest(
            "Invalid Parameter",
            "Schedule ID and optional 'start' time must be integers.",
        )

    # Use a single 'with' block for the entire operation including temp table
    temp_table_name = TEMP_EVENT_TABLE  # Initialize here
    try:
        with db.connect() as connection:
            cursor = None  # Initialize cursor
            try:
                cursor = connection.cursor(db.DictCursor)

                # 1. Fetch schedule info (including team_id and scheduler name)
                # Using get_schedules which should handle DB access correctly
                schedules_list = get_schedules(
                    {"id": schedule_id_int}, dbinfo=(connection, cursor)
                )
                if not schedules_list:
                    raise HTTPNotFound(
                        description=f"Schedule {schedule_id_int} not found."
                    )
                schedule = schedules_list[0]
                team_name = schedule.get(
                    "team"
                )  # Needed for build_preview_response
                if not team_name:
                    raise HTTPInternalServerError(
                        title="Data Error",
                        description=f"Schedule {schedule_id_int} missing team information.",
                    )

                # 2. Load Scheduler Module
                scheduler_info = schedule.get("scheduler", {})
                scheduler_name = (
                    scheduler_info.get("name")
                    if isinstance(scheduler_info, dict)
                    else None
                )
                if not scheduler_name:
                    raise HTTPInternalServerError(
                        title="Configuration Error",
                        description=f"Schedule {schedule_id_int} has no associated scheduler.",
                    )

                try:
                    scheduler = load_scheduler(scheduler_name)
                except Exception as e:
                    logger.error(
                        f"Error loading scheduler '{scheduler_name}': {e}",
                        exc_info=True,
                    )
                    raise HTTPInternalServerError(
                        title="Scheduler Load Error",
                        description=f"Failed to load scheduler module '{scheduler_name}'.",
                    )
                # --- Temporary Table Logic ---
                # Ensure temp table name is safe (already hardcoded, so it is)
                # temp_table_name is already initialized before the outer try block

                # 3. Drop existing temp table (if any) - important for idempotency within session
                # Use try-except for DROP IF EXISTS as it might not exist
                # Use try-except for DROP IF EXISTS as it might not exist
                try:
                    cursor.execute(
                        f"DROP TEMPORARY TABLE IF EXISTS `{temp_table_name}`"
                    )
                except db.Error as drop_err:
                    # Log warning, but proceed. Might fail if DB user lacks permissions.
                    logger.warning(
                        f"Could not drop temporary table '{temp_table_name}': {drop_err}"
                    )

                # 4. Create Temporary Table (schema should match 'event' table closely)
                # Adjusted schema based on potential needs for preview
                query_create_temp = f"""
                    CREATE TEMPORARY TABLE `{temp_table_name}` (
                        `id` int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `team_id` int(10) unsigned NOT NULL,
                        `role_id` int(10) unsigned NOT NULL,
                        `schedule_id` int(10) unsigned DEFAULT NULL,
                        `link_id` varchar(128) DEFAULT NULL,
                        `user_id` int(10) unsigned NOT NULL,
                        `start` bigint(20) NOT NULL,
                        `end` bigint(20) NOT NULL,
                        `note` text DEFAULT NULL,
                        INDEX `temp_event_user_idx` (`user_id`),
                        INDEX `temp_event_team_role_idx` (`team_id`, `role_id`),
                        INDEX `temp_event_time_idx` (`start`, `end`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(query_create_temp)

                # 5. Populate the temporary table using the scheduler's populate method
                # Pass the temp table name to the populate method
                # The populate method should handle commit/rollback for its operations *within* the transaction
                # Note: populate might raise HTTPBadRequest if start_time is in the past.
                scheduler.populate(
                    schedule,
                    start_time,
                    (connection, cursor),
                    table_name=temp_table_name,
                )

                # 6. Build the preview response using data from the temporary table
                # Calculate time range for preview (e.g., from start_time to threshold)
                preview_end_time = (
                    start_time
                    + schedule.get("auto_populate_threshold", 21) * 24 * 60 * 60
                )  # Default 21 days

                # Call build_preview_response which queries the temp table
                preview_json = scheduler.build_preview_response(
                    cursor,
                    start__lt=preview_end_time,  # Events ending after start_time
                    end__ge=start_time,  # Events starting before preview_end_time
                    team__eq=team_name,  # Filter by team name
                    table_name=temp_table_name,
                )

                # 7. Transaction is implicitly committed here if no exceptions occurred
                # OR rolled back by the 'with' block if an exception occurred.
                # No explicit commit needed here as populate handles its internal commits,
                # and the main goal is reading from the temp table which exists for the session.

                resp.text = preview_json
                resp.status = HTTP_200

            except HTTPError as http_e:
                # Log and re-raise known HTTP errors
                logger.info(
                    f"HTTP error during preview for schedule {schedule_id_int}: {http_e.title} - {http_e.description}"
                )
                raise http_e
            except db.Error as db_e:
                # Log DB errors during the process
                logger.exception(
                    f"Database error during preview for schedule {schedule_id_int}: {db_e}"
                )
                # Rollback is handled by 'with' block
                raise HTTPInternalServerError(
                    title="Database Error",
                    description=f"A database error occurred during preview generation: {db_e}",
                )
            except Exception as e:
                # Log unexpected errors
                logger.exception(
                    f"Unexpected error during preview for schedule {schedule_id_int}: {e}"
                )
                # Rollback is handled by 'with' block
                raise HTTPInternalServerError(
                    title="Preview Error",
                    description=f"An unexpected error occurred during preview generation: {e}",
                )
            finally:
                # Attempt to drop the temp table explicitly, though it should drop on session end
                if cursor:
                    try:
                        # Check if connection is still valid before executing drop
                        if (
                            connection and not connection._raw_conn.closed
                        ):  # Accessing protected member, might need adjustment based on actual connection object
                            cursor.execute(
                                f"DROP TEMPORARY TABLE IF EXISTS `{temp_table_name}`"
                            )
                            logger.debug(
                                f"Dropped temporary table '{temp_table_name}'"
                            )
                    except Exception as drop_final_err:
                        logger.warning(
                            f"Error during final drop of temporary table '{temp_table_name}': {drop_final_err}"
                        )
                    finally:
                        # Ensure cursor is closed
                        try:
                            cursor.close()
                        except Exception as cur_e:
                            logger.warning(
                                f"Error closing cursor in finally block: {cur_e}"
                            )
        # Connection automatically closed/returned by 'with'

    except db.Error as e:
        logger.exception(
            f"Database connection error for preview schedule {schedule_id_int}: {e}"
        )
        raise HTTPInternalServerError(description="Database connection failed.")
    except Exception as e:
        logger.exception(
            f"Unexpected error in preview endpoint for schedule {schedule_id_int}: {e}"
        )
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )
