# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import uuid

from ... import db


def generate_ical_key():
    """Generates a new unique UUID4 key."""
    return str(uuid.uuid4())


def check_ical_team(team, requester):
    """
    Checks if a team exists and is active.
    Currently, we allow users to request ical key for any active team calendar.
    Args:
        team (str): The name of the team.
        requester (str): The name of the user requesting (currently unused in check).

    Returns:
        bool: True if the team exists and is active, False otherwise.
    """
    team_exist_and_active = False
    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT 1  -- Select a constant, we only care about row count
            FROM `team`
            WHERE `name` = %s AND `active` = TRUE
            """,
            (team,),
        )
        # Check rowcount within the 'with' block
        team_exist_and_active = cursor.rowcount > 0
        # Connection and cursor closed automatically
    return team_exist_and_active


def check_ical_key_requester(key, requester):
    """
    Checks if the given key belongs to the specified requester.

    Args:
        key (str): The iCal key to check.
        requester (str): The user name expected to own the key.

    Returns:
        bool: True if the key exists and belongs to the requester, False otherwise.
    """
    is_requester = False
    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT 1 -- Select a constant, we only care about row count
            FROM `ical_key`
            WHERE `key` = %s AND `requester` = %s
            """,
            (key, requester),
        )
        # Check rowcount within the 'with' block
        is_requester = cursor.rowcount > 0
        # Connection and cursor closed automatically
    return is_requester


def get_name_and_type_from_key(key):
    """
    Retrieves the associated name (team/user) and type ('team'/'user') for a given iCal key.

    Args:
        key (str): The iCal key.

    Returns:
        tuple | None: A tuple (name, type) if the key is found, otherwise None.
    """
    result = None
    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT `name`, `type`
            FROM `ical_key`
            WHERE `key` = %s
            """,
            (key,),
        )
        if cursor.rowcount > 0:
            row = cursor.fetchone()
            result = (row[0], row[1])  # Access by index for default cursor
        # Connection and cursor closed automatically
    return result


def get_ical_key(requester, name, type):
    """
    Retrieves an existing iCal key for a specific requester, name, and type.

    Args:
        requester (str): The user who requested the key.
        name (str): The name (team/user) associated with the key.
        type (str): The type ('team'/'user') associated with the key.

    Returns:
        str | None: The iCal key if found, otherwise None.
    """
    key = None
    with db.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT `key`
            FROM `ical_key`
            WHERE
                `requester` = %s AND
                `name` = %s AND
                `type` = %s
            """,
            (requester, name, type),
        )
        if cursor.rowcount > 0:
            key = cursor.fetchone()[0]  # Access by index for default cursor
        # Connection and cursor closed automatically
    return key


def update_ical_key(requester, name, type, key):
    """
    Inserts a new iCal key or updates the timestamp of an existing entry
    for the given requester, name, and type combination.

    Args:
        requester (str): The user requesting the key.
        name (str): The name (team/user) associated with the key.
        type (str): The type ('team'/'user') associated with the key.
        key (str): The iCal key (UUID) to insert or update.
    """
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO `ical_key` (`key`, `requester`, `name`, `type`, `time_created`)
                VALUES (%s, %s, %s, %s, UNIX_TIMESTAMP())
                ON DUPLICATE KEY UPDATE `key` = VALUES(`key`), `time_created` = UNIX_TIMESTAMP()
                """,  # Use VALUES(`key`) for clarity on update
                (key, requester, name, type),  # Parameter tuple for INSERT part
                # The ON DUPLICATE part uses VALUES() syntax, no extra params needed usually
                # Original code passed key twice, which works but VALUES() is clearer
                # If using older MySQL/MariaDB without VALUES(), original (key,) might be needed:
                # (key, requester, name, type, key) -- uncomment if VALUES() fails
            )
            connection.commit()  # Commit changes within the 'with' block
        except db.Error as e:  # Catch potential DB errors
            connection.rollback()  # Rollback on error
            # Log error or re-raise appropriately
            print(f"Error updating ical key: {e}")  # Basic error printing
            raise  # Re-raise the exception after rollback
        # Connection and cursor closed automatically


def delete_ical_key(requester, name, type):
    """
    Deletes an iCal key based on the requester, name, and type.

    Args:
        requester (str): The user who requested the key.
        name (str): The name (team/user) associated with the key.
        type (str): The type ('team'/'user') associated with the key.
    """
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                DELETE FROM `ical_key`
                WHERE
                    `requester` = %s AND
                    `name` = %s AND
                    `type` = %s
                """,
                (requester, name, type),
            )
            connection.commit()  # Commit deletion within the 'with' block
        except db.Error as e:  # Catch potential DB errors
            connection.rollback()  # Rollback on error
            # Log error or re-raise appropriately
            print(f"Error deleting ical key: {e}")  # Basic error printing
            raise  # Re-raise the exception after rollback
        # Connection and cursor closed automatically


def get_ical_key_detail(key):
    """
    Retrieves details (requester, name, type, time_created) for a given iCal key.

    Args:
        key (str): The iCal key.

    Returns:
        list: A list of dictionaries containing key details. It might contain
              more than one entry in the rare case of a UUID collision.
    """
    results = []
    with db.connect() as connection:
        # Ensure DictCursor is available
        if not db.DictCursor:
            raise RuntimeError(
                "DictCursor is required but not available. Check DBAPI driver and db.init()."
            )
        cursor = connection.cursor(db.DictCursor)
        cursor.execute(
            """
            SELECT `requester`, `name`, `type`, `time_created`
            FROM `ical_key`
            WHERE `key` = %s
            """,
            (key,),
        )
        results = cursor.fetchall()
        # Connection and cursor closed automatically
    return results


def get_ical_key_detail_by_requester(requester):
    """
    Retrieves details for all iCal keys associated with a specific requester.

    Args:
        requester (str): The user whose keys are to be retrieved.

    Returns:
        list: A list of dictionaries, each containing details of an iCal key.
    """
    results = []
    with db.connect() as connection:
        # Ensure DictCursor is available
        if not db.DictCursor:
            raise RuntimeError(
                "DictCursor is required but not available. Check DBAPI driver and db.init()."
            )
        cursor = connection.cursor(db.DictCursor)
        cursor.execute(
            """
            SELECT `key`, `name`, `type`, `time_created`
            FROM `ical_key`
            WHERE `requester` = %s
            """,
            (requester,),
        )
        results = cursor.fetchall()
        # Connection and cursor closed automatically
    return results


def invalidate_ical_key(key):
    """
    Deletes an iCal key entry directly by its key value.

    Args:
        key (str): The iCal key (UUID) to delete.
    """
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                DELETE FROM `ical_key`
                WHERE `key` = %s
                """,
                (key,),
            )
            connection.commit()  # Commit deletion within the 'with' block
        except db.Error as e:  # Catch potential DB errors
            connection.rollback()  # Rollback on error
            # Log error or re-raise appropriately
            print(f"Error invalidating ical key: {e}")  # Basic error printing
            raise  # Re-raise the exception after rollback
        # Connection and cursor closed automatically


def invalidate_ical_key_by_requester(requester):
    """
    Deletes all iCal key entries associated with a specific requester.

    Args:
        requester (str): The user whose keys are to be deleted.
    """
    with db.connect() as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                DELETE FROM `ical_key`
                WHERE `requester` = %s
                """,
                (requester,),
            )
            connection.commit()  # Commit deletion within the 'with' block
        except db.Error as e:  # Catch potential DB errors
            connection.rollback()  # Rollback on error
            # Log error or re-raise appropriately
            print(
                f"Error invalidating ical keys by requester: {e}"
            )  # Basic error printing
            raise  # Re-raise the exception after rollback
        # Connection and cursor closed automatically
