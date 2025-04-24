# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from datetime import datetime as dt

from icalendar import Calendar, Event, vCalAddress, vText
from pytz import utc

from ... import db


def events_to_ical(events, identifier, contact=True):
    """
    Converts a list of event dictionaries into an iCalendar string.
    Fetches user details (full name, contacts) from the database for each unique user.

    :param events: List of event dictionaries
    :param identifier: Identifier for the calendar (e.g., team name)
    :param contact: Whether to include user contact information in the iCal description/attendee
    :return: iCalendar string
    """
    # Use the 'with' statement for safe connection management for user lookups
    with db.connect() as connection:
        cursor = connection.cursor(
            db.DictCursor
        )  # Use DictCursor for fetching user details

        # Initialize iCal Calendar and a cache for user info
        ical = Calendar()
        ical.add("calscale", "GREGORIAN")
        ical.add("prodid", "-//Oncall//Oncall calendar feed//EN")
        ical.add("version", "2.0")
        ical.add("x-wr-calname", "%s Oncall Calendar" % identifier)

        users_cache = {}  # Cache for user information fetched from DB

        for event in events:
            username = event.get("user")  # Use .get for safety
            if not username:
                # Skip events with no user name, or log a warning
                continue  # Or log a warning if this indicates a data issue

            # Check if user info is already in the cache
            if username not in users_cache:
                # User info not in cache, fetch from DB
                if contact:
                    # Query to get full name and contact details
                    cursor.execute(
                        """
                        SELECT
                            `user`.`full_name` AS full_name,
                            `contact_mode`.`name` AS contact_mode,
                            `user_contact`.`destination` AS destination
                        FROM `user`
                        LEFT JOIN `user_contact` ON `user`.`id` = `user_contact`.`user_id`
                        LEFT JOIN `contact_mode` ON `contact_mode`.`id` = `user_contact`.`mode_id`
                        WHERE `user`.`name` = %s
                    """,
                        (username,),  # Parameterize username as a tuple
                    )
                else:
                    # Query to get only full name
                    cursor.execute(
                        """
                        SELECT `user`.`full_name` AS full_name
                        FROM `user`
                        WHERE `user`.`name` = %s
                    """,
                        (username,),  # Parameterize username as a tuple
                    )

                # Fetch results for this user
                user_rows = cursor.fetchall()

                info = {"username": username, "contacts": {}}
                # If user found (user_rows is not empty)
                if user_rows:
                    # Full name is the same for all rows for this user
                    info["full_name"] = user_rows[0].get(
                        "full_name", username
                    )  # Use .get, fallback to username

                    if contact:
                        # Populate contacts from fetched rows
                        for row in user_rows:
                            mode = row.get("contact_mode")
                            dest = row.get("destination")
                            # Only add contact if mode and destination are not None (handles LEFT JOIN results)
                            if mode is not None and dest is not None:
                                info["contacts"][mode] = dest
                else:
                    # User not found in DB - use username as full name
                    info["full_name"] = username

                # Store user info in cache
                users_cache[username] = info

            # Retrieve user info from cache for creating the event
            user = users_cache[username]

            # Create the iCal event itself
            full_name = user.get(
                "full_name", user["username"]
            )  # Get full name from info dict
            cal_event = Event()
            cal_event.add(
                "uid", "event-%s@oncall" % event.get("id", "unknown")
            )  # Use .get for event id safety
            cal_event.add(
                "dtstart",
                (
                    dt.fromtimestamp(event.get("start"), utc)
                    if event.get("start") is not None
                    else None
                ),
            )  # Handle potential None start
            cal_event.add(
                "dtend",
                (
                    dt.fromtimestamp(event.get("end"), utc)
                    if event.get("end") is not None
                    else None
                ),
            )  # Handle potential None end
            cal_event.add("dtstamp", dt.utcnow())  # Use UTC now for timestamp
            cal_event.add(
                "summary",
                "%s %s shift: %s"
                % (
                    event.get("team", "N/A"),
                    event.get("role", "N/A"),
                    full_name,
                ),  # Use .get for event fields
            )

            # Prepare description based on contact flag
            description_parts = [full_name]
            if (
                contact and user["contacts"]
            ):  # Add contacts only if contact is True and contacts exist
                description_parts.extend(
                    [
                        f"{mode}: {dest}"
                        for mode, dest in user["contacts"].items()
                    ]
                )
            cal_event.add("description", "\n".join(description_parts))

            cal_event.add("TRANSP", "TRANSPARENT")

            # Attach info about the user oncall as attendee
            attendee_email = (
                user["contacts"].get("email") if contact else ""
            )  # Get email if contact and exists
            attendee = vCalAddress(
                f"MAILTO:{attendee_email}"
            )  # Use f-string for email
            attendee.params["cn"] = vText(full_name)
            attendee.params["ROLE"] = vText("REQ-PARTICIPANT")
            cal_event.add("attendee", attendee, encode=0)

            # Add the created event to the calendar
            ical.add_component(cal_event)

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.
        # The 'ical' object is still available after the 'with' block.

    # Convert the iCal Calendar object to iCalendar string format and return
    return ical.to_ical()
