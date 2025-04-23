#!/usr/bin/env python

# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import importlib
import logging
import logging.handlers
import os

# -*- coding:utf-8 -*-
import sys
import time
from collections import defaultdict

from oncall import db, utils
from oncall.api.v0.schedules import get_schedules


def load_scheduler(scheduler_name):
    return importlib.import_module(
        "oncall.scheduler." + scheduler_name
    ).Scheduler()


def main():
    config = utils.read_config(sys.argv[1])
    try:
        db.init(config["db"])
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}", exc_info=True)
        sys.exit(1)

    cycle_time = config.get("scheduler_cycle_time", 3600)

    while True:
        start = time.time()
        try:
            # Use 'with' statement for connection management
            with db.connect() as connection:
                db_cursor = None
                try:
                    db_cursor = connection.cursor(db.DictCursor)

                    # Load all schedulers
                    db_cursor.execute("SELECT name FROM scheduler")
                    schedulers = {}
                    for row in db_cursor:
                        try:
                            scheduler_name = row["name"]
                            if scheduler_name not in schedulers:
                                schedulers[scheduler_name] = load_scheduler(
                                    scheduler_name
                                )
                        except (ImportError, AttributeError):
                            logger.exception(
                                "Failed to load scheduler %s, skipping",
                                row["name"],
                            )

                    # Iterate through all teams
                    db_cursor.execute(
                        "SELECT id, name, scheduling_timezone FROM team WHERE active = TRUE"
                    )
                    teams = db_cursor.fetchall()
                    for team in teams:
                        logger.info("scheduling for team: %s", team["name"])
                        schedule_map = defaultdict(list)
                        # Pass connection and cursor via dbinfo tuple
                        # get_schedules is assumed to use the provided dbinfo correctly
                        team_schedules = get_schedules(
                            {"team_id": team["id"]},
                            dbinfo=(connection, db_cursor),
                        )

                        for schedule in team_schedules:
                            # Ensure 'scheduler' key exists and has a 'name' subkey
                            scheduler_info = schedule.get("scheduler", {})
                            scheduler_name = scheduler_info.get("name") if isinstance(scheduler_info, dict) else None

                            if scheduler_name and scheduler_name in schedulers:
                                schedule_map[scheduler_name].append(schedule)
                            elif scheduler_name:
                                logger.warning(
                                    f"Scheduler '{scheduler_name}' defined for schedule {schedule.get('id')} but not loaded/found. Skipping."
                                )
                            else:
                                logger.warning(
                                    f"Schedule {schedule.get('id')} has missing or invalid scheduler information. Skipping."
                                )

                        for scheduler_name, schedules_list in schedule_map.items():
                            if schedules_list: # Only run if there are schedules
                                # Pass connection and cursor via dbinfo tuple
                                # The schedule method handles its own commit/rollback within the transaction
                                schedulers[scheduler_name].schedule(
                                    team,
                                    schedules_list,
                                    (connection, db_cursor),
                                )
                    # Note: Commits are handled within the scheduler.schedule method per scheduler run

                except Exception:
                    # Log exceptions occurring within the transaction
                    logger.exception("Error during scheduling cycle")
                    # Rollback is handled automatically by the 'with' block on exception
                    # No explicit rollback needed here.
                    # Re-raise the exception if needed, or handle appropriately
                    # For the main loop, we might want to log and continue to the sleep phase
                finally:
                    # Cursor closure is good practice, though often handled by connection close
                    if db_cursor:
                        try:
                            db_cursor.close()
                        except Exception as cur_e:
                            logger.warning(f"Error closing cursor: {cur_e}")
            # Connection is automatically closed/returned to pool by 'with' statement exit

        except db.Error as e:
            logger.exception(f"Database connection error in main loop: {e}")
            # Wait before retrying on connection error
            time.sleep(60)
        except Exception as e:
            logger.exception(f"Unexpected error in main loop: {e}")
            # Wait before retrying on other errors
            time.sleep(60)

        # Sleep until next time
        elapsed = time.time() - start
        sleep_time = cycle_time - elapsed
        if sleep_time > 0:
            logger.info("Scheduling cycle finished in %.2f seconds. Sleeping for %.2f seconds", elapsed, sleep_time)
            time.sleep(sleep_time)
        else:
            logger.warning(
                "Scheduling cycle took %.2f seconds (longer than cycle time %s), skipping sleep",
                elapsed, cycle_time
            )


if __name__ == "__main__":
    # Setup logging (moved from global scope to ensure it runs when script is executed)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    log_file = os.environ.get("SCHEDULER_LOG_FILE")
    if log_file:
        # Use TimedRotatingFileHandler for better rotation based on time
        ch = logging.handlers.TimedRotatingFileHandler(log_file, when='midnight', backupCount=10)
    else:
        ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    # Configure root logger
    logging.basicConfig(level=logging.INFO, handlers=[ch])
    # Get logger for this module
    logger = logging.getLogger(__name__) # Use __name__ for module-specific logger

    main()
