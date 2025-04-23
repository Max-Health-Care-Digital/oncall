# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import operator

from falcon import (
    HTTPBadRequest,
    HTTPError,
    HTTPInternalServerError,
    HTTPNotFound,
)

# Assuming load_scheduler is correctly imported from oncall.bin.scheduler
# Adjust the import path if necessary
try:
    from oncall.bin.scheduler import load_scheduler
except ImportError:
    # Provide a fallback or raise a more specific error if scheduler loading is critical
    def load_scheduler(name):
        raise ImportError(
            f"Failed to import load_scheduler. Scheduler '{name}' cannot be loaded."
        )


from ... import db
from .schedules import (
    get_schedules,
)  # Assuming this uses 'with' internally or is refactored

# Define constants for temporary table name
TEMP_EVENT_TABLE = "temp_event_preview"  # Use a more specific name perhaps


def on_get(req, resp, schedule_id):
    """
    Run the scheduler on demand from a given point in time to generate a preview.
    Unlike populate, it doesn't permanently delete or insert anything into main tables.

    Query Params:
        start (float): Required. Unix timestamp for the start of the preview generation.
        start__lt (Not used directly here?): Parameter likely for scheduler's response building.
        end__ge (Not used directly here?): Parameter likely for scheduler's response building.
        team__eq (Not used directly here?): Parameter likely for scheduler's response building.
    """
    # Validate and get parameters first
    try:
        # Ensure schedule_id is int
        schedule_id_int = int(schedule_id)
        start_time = float(req.get_param("start", required=True))
        # These seem intended for the scheduler's response builder, store them
        start__lt = req.get_param("start__lt", required=True)
        end__ge = req.get_param("end__ge", required=True)
        team__eq = req.get_param("team__eq", required=True)
    except ValueError as e:
        raise HTTPBadRequest(
            "Invalid Parameter",
            f"Invalid number format in request parameters: {e}",
        )
    except Exception as e:
        raise HTTPBadRequest(
            "Missing Parameter",
            f"Required query parameter missing or invalid: {e}",
        )

    last_end = 0  # Default value
    scheduler = None
    schedule = None
    team_id = None
    cursor = None  # Define cursor in outer scope for finally block

    try:
        with db.connect() as connection:
            # Use DictCursor if needed by subsequent processing
            cursor = connection.cursor(db.DictCursor)
            temp_table_created = False  # Flag to track if temp table exists

            try:
                # 1. Get Scheduler Name
                cursor.execute(
                    """SELECT `scheduler`.`name`, `schedule`.`team_id`
                       FROM `schedule`
                       JOIN `scheduler` ON `schedule`.`scheduler_id` = `scheduler`.`id`
                       WHERE `schedule`.`id` = %s""",
                    (schedule_id_int,),  # Use tuple for parameter
                )
                schedule_info = cursor.fetchone()
                if not schedule_info:
                    raise HTTPNotFound(
                        description=f"Schedule {schedule_id_int} not found."
                    )

                scheduler_name = schedule_info["name"]
                team_id = schedule_info["team_id"]  # Get team_id directly here

                # 2. Load Scheduler Module (Can potentially be moved outside `with` if no DB needed)
                try:
                    scheduler = load_scheduler(scheduler_name)
                except Exception as e:
                    # Handle errors during scheduler loading
                    print(f"Error loading scheduler '{scheduler_name}': {e}")
                    raise HTTPInternalServerError(
                        title="Scheduler Load Error",
                        description=f"Failed to load scheduler module '{scheduler_name}'.",
                    )

                # 3. Get Full Schedule Details (assuming get_schedules handles its own DB connection)
                # If get_schedules needs the *same* connection/cursor, it should be refactored
                # to accept `cursor` as an argument.
                schedules_list = get_schedules(
                    filter_params={"id": schedule_id_int},
                    dbinfo=(connection, cursor),
                )  # Pass cursor if needed by get_schedules
                if not schedules_list:
                    # Should have been caught above, but for safety
                    raise HTTPNotFound(
                        description=f"Schedule {schedule_id_int} details not found."
                    )
                schedule = schedules_list[0]
                # team_id = schedule["team_id"] # Already fetched above

                # 4. Calculate earliest relevant end time (`last_end`)
                query_last_end = """
                    SELECT `user_id`, MAX(`end`) AS `last_end` FROM `event`
                    WHERE (`team_id` = %s OR `team_id` IN (SELECT `subscription_id` FROM `team_subscription` WHERE `team_id` = %s))
                      AND `end` <= %s
                    GROUP BY `user_id`
                    """
                cursor.execute(query_last_end, (team_id, team_id, start_time))
                last_end_results = cursor.fetchall()
                if last_end_results:
                    # Find the minimum of the maximum end times per user
                    last_end = min(
                        last_end_results, key=operator.itemgetter("last_end")
                    )["last_end"]

                # 5. Create Temporary Table with relevant events
                # IMPORTANT: Temporary tables are often session-scoped. Ensure it's dropped.
                # Using f-string for table name is safe here as it's controlled internally.
                query_create_temp = f"""
                    CREATE TEMPORARY TABLE `{TEMP_EVENT_TABLE}` (
                        `id` int(10) unsigned NOT NULL, `team_id` int(10) unsigned NOT NULL,
                        `role_id` int(10) unsigned NOT NULL, `schedule_id` int(10) unsigned DEFAULT NULL,
                        `link_id` varchar(128) DEFAULT NULL, `user_id` int(10) unsigned NOT NULL,
                        `start` int(10) unsigned NOT NULL, `end` int(10) unsigned NOT NULL,
                        `note` text,
                        PRIMARY KEY (`id`), -- Add primary key if useful for performance
                        INDEX `idx_user_end` (`user_id`, `end`), -- Example index
                        INDEX `idx_end` (`end`) -- Example index
                    ) ENGINE=InnoDB AS (
                        SELECT DISTINCT `event`.`id`, `event`.`team_id`, `event`.`role_id`,
                               `event`.`schedule_id`, `event`.`link_id`, `event`.`user_id`,
                               `event`.`start`, `event`.`end`, `event`.`note`
                        FROM `event`
                        INNER JOIN `roster_user` ON `event`.`user_id` = `roster_user`.`user_id`
                        WHERE `roster_user`.`roster_id` IN (
                                  SELECT `id` FROM `roster` WHERE `team_id` = %s OR `team_id` IN (
                                      SELECT `subscription_id` FROM `team_subscription` WHERE `team_id` = %s
                                  )
                              )
                          AND `event`.`end` >= %s
                    )
                    """
                cursor.execute(query_create_temp, (team_id, team_id, last_end))
                temp_table_created = True  # Mark table as created

                # 6. Run Scheduler's populate method using the temp table
                # Pass connection/cursor tuple as required by the original code
                scheduler.populate(
                    schedule, start_time, (connection, cursor), TEMP_EVENT_TABLE
                )

                # 7. Build the response using the temp table data
                resp.text = scheduler.build_preview_response(
                    cursor, start__lt, end__ge, team__eq, TEMP_EVENT_TABLE
                )
                # Note: No commit needed as we only created/read a temp table

            except HTTPError as e:
                # Re-raise specific HTTP errors caught during processing
                raise e
            except Exception as e:
                # Catch any other error during the process
                print(f"Error during schedule preview generation: {e}")  # Log
                raise HTTPInternalServerError(
                    description=f"Failed to generate schedule preview: {e}"
                ) from e
            finally:
                # Ensure temporary table is dropped even if errors occur after creation
                if cursor and temp_table_created:
                    try:
                        cursor.execute(
                            f"DROP TEMPORARY TABLE IF EXISTS `{TEMP_EVENT_TABLE}`"
                        )
                    except Exception as drop_e:
                        # Log error during drop, but don't mask original error
                        print(
                            f"Error dropping temporary table {TEMP_EVENT_TABLE}: {drop_e}"
                        )

            # Connection automatically closed by 'with' statement here

    except db.Error as e:
        # Handle connection errors
        print(f"Database connection error in on_get schedule preview: {e}")
        raise HTTPInternalServerError(description="Database connection failed.")
    except HTTPError as e:
        # Re-raise HTTP errors from initial param checks or caught above
        raise e
    except Exception as e:
        # Handle other unexpected errors
        print(f"Unexpected error in on_get schedule preview: {e}")
        raise HTTPInternalServerError(
            description=f"An unexpected error occurred: {e}"
        )

    # resp.status is implicitly 200 OK if no exception is raised
