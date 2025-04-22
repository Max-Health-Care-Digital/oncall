# Import necessary libraries
import base64
import hashlib
import hmac
import logging
import sys
from typing import Any, Dict

import boto3  # Added for AWS Cognito
from botocore.exceptions import ClientError  # Added for Boto3 error handling
from sqlalchemy.exc import SQLAlchemyError

from oncall import db
from oncall.user_sync.ldap_sync import LDAP_SETTINGS, get_oncall_user, stats

logging.basicConfig(
    level=logging.DEBUG,  # Set the minimum level of messages to log (e.g., DEBUG, INFO, WARNING)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Define the log message format
    # stream=sys.stdout  # Uncomment this line to send logs to standard output instead of error
    stream=sys.stderr,  # Send logs to standard error (default if stream is None)
)
logger = logging.getLogger(__name__)


class Authenticator:
    def __init__(self, config):
        """
        Initializes the Authenticator.

        Reads configuration and sets up the authentication method.
        Expects Cognito configuration in the 'config' dictionary if not in debug mode.

        Args:
            config (dict): Configuration dictionary. Expected keys for Cognito:
                           'aws_region', 'cognito_user_pool_id', 'cognito_app_client_id'.
                           Optional: 'debug' (boolean).
        """
        if config.get("debug"):
            logger.info("Using debug authentication.")
            self.authenticate = self.debug_auth
            return

        # --- Cognito Setup ---

        self.aws_region = config.get("aws_region")
        self.user_pool_id = config.get("cognito_user_pool_id")
        self.app_client_id = config.get("cognito_app_client_id")
        self.app_client_secret = config.get("cognito_app_client_secret")

        if not all([self.aws_region, self.user_pool_id, self.app_client_id]):
            logger.error(
                "Missing one or more Cognito configuration parameters: "
                "'aws_region', 'cognito_user_pool_id', 'cognito_app_client_id'"
            )
            # Raise an error or handle appropriately - preventing startup might be best
            raise ValueError("Incomplete Cognito configuration provided.")

        try:
            # Initialize Boto3 Cognito client
            self.cognito_client = boto3.client(
                "cognito-idp", region_name=self.aws_region
            )
            logger.info(
                f"Cognito client initialized for region {self.aws_region} and pool {self.user_pool_id}"
            )
            self.authenticate = (
                self.cognito_auth
            )  # Set the authentication method

        except Exception as e:
            logger.error(
                f"Failed to initialize Boto3 Cognito client: {e}", exc_info=True
            )
            # Depending on requirements, you might want to fall back to debug or raise an exception
            raise RuntimeError(f"Failed to initialize Cognito client: {e}")

    def _generate_secret_hash(self, username, key, app_client_id):
        message = bytes(username + app_client_id, "utf-8")
        key = bytes(key, "utf-8")
        secret_hash = base64.b64encode(
            hmac.new(key, message, digestmod=hashlib.sha256).digest()
        ).decode()
        return secret_hash

    def cognito_auth(self, username, password):
        """
        Authenticates a user against AWS Cognito using the AdminInitiateAuth flow.

        Args:
            username (str): The username to authenticate.
            password (str): The user's password.

        Returns:
            bool: True if authentication is successful, False otherwise.
            None: Can optionally be returned on specific server/config errors,
                  though returning False is often sufficient for login flows.
        """
        ldap_contacts = {}

        if not password:
            logger.warning(
                f"Authentication attempt failed for user '{username}': No password provided."
            )
            return False

        try:
            logger.debug(
                f"Attempting Cognito authentication for user: {username}"
            )
            client_secret = self.app_client_secret

            secret_hash = self._generate_secret_hash(
                username, client_secret, self.app_client_id
            )

            response = self.cognito_client.initiate_auth(
                ClientId=self.app_client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={
                    "USERNAME": username,
                    "PASSWORD": password,
                    "SECRET_HASH": secret_hash,
                },
            )

            # Check if authentication was successful (response has AuthenticationResult)
            if response.get("AuthenticationResult"):
                from oncall import db

                logger.info(
                    f"Cognito authentication successful for user: {username}"
                )
                ldap_contacts = {}

                try:
                    # Use the new db.connect() which returns the wrapper
                    with db.connect() as connection_wrapper:
                        # Get cursor from the wrapper's .cursor() method
                        # Use nested 'with' for cursor cleanup if the cursor supports it
                        # (DBAPI standard doesn't guarantee cursor context mgmt, but many drivers add it)
                        # Safer manual close is also an option if needed.
                        try:
                            cursor = connection_wrapper.cursor(
                                db.DictCursor
                            )  # Use DictCursor if available
                            # Pass the cursor to helper functions
                            if user_exists(username, cursor):
                                logger.info(
                                    "user %s exists, updating.", username
                                )
                                update_user(username, ldap_contacts, cursor)
                            else:
                                logger.info(
                                    "user %s does not exist, importing.",
                                    username,
                                )
                                import_user(username, ldap_contacts, cursor)

                            # Commit using the wrapper *before* exiting 'with' block
                            connection_wrapper.commit()
                            logger.info(
                                f"Database changes for user '{username}' committed."
                            )
                        finally:
                            # Explicitly close cursor if not using 'with' for it
                            if "cursor" in locals() and cursor:
                                try:
                                    cursor.close()
                                except Exception as cur_e:
                                    logger.warning(
                                        f"Error closing cursor: {cur_e}",
                                        exc_info=True,
                                    )

                except (db.Error, SQLAlchemyError) as db_err:
                    logger.error(
                        f"Database error during user import/update for '{username}': {db_err}",
                        exc_info=True,
                    )
                    # Consider rollback if appropriate, via connection_wrapper.rollback() ?
                    # Needs careful thought about transaction state after error.
                except Exception as e:
                    logger.error(
                        f"Unexpected error during user import/update for '{username}': {e}",
                        exc_info=True,
                    )

                return True  # Auth success regardless of import outcome            else:
            else:
                # This case might indicate an issue or a challenge response,
                # but for simple password auth, lack of AuthenticationResult means failure.
                logger.warning(
                    f"Cognito authentication failed for user '{username}': No AuthenticationResult in response."
                )
                return False

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NotAuthorizedException":
                print(e.response)
                logger.warning(
                    f"Cognito authentication failed for user '{username}': Invalid credentials."
                )
                return False
            elif error_code == "UserNotFoundException":
                logger.warning(
                    f"Cognito authentication failed for user '{username}': User not found."
                )
                return False
            elif error_code == "ResourceNotFoundException":
                logger.error(
                    f"Cognito configuration error: User Pool ({self.user_pool_id}) "
                    f"or App Client ({self.app_client_id}) not found in region {self.aws_region}.",
                    exc_info=True,
                )
                return False  # Or potentially None, or raise an exception
            elif error_code == "InvalidParameterException":
                logger.error(
                    f"Cognito API call failed: Invalid parameters provided. Check configuration and inputs.",
                    exc_info=True,
                )
                return False  # Or potentially None
            else:
                # Log other Boto3/Cognito client errors
                logger.error(
                    f"An unexpected Cognito error occurred for user '{username}': {e}",
                    exc_info=True,
                )
                # Decide on return value. False is usually safest for login. None could signal a server issue.
                return False  # Or return None if login.py should handle None differently
        except Exception as e:
            # Catch potential non-Boto3 exceptions (network issues, etc.)
            logger.error(
                f"An unexpected error occurred during Cognito authentication for user '{username}': {e}",
                exc_info=True,
            )
            return False  # Or return None

    def debug_auth(self, username, password):
        """
        Debug authentication: always returns True.
        """
        logger.debug(f"Debug authentication successful for user: {username}")
        return True


def user_exists(username: str, cursor: Any) -> bool:
    """
    Checks if a user exists in the database using a DBAPI cursor.

    Args:
        username: The username to check.
        cursor: An active DBAPI cursor object (preferably DictCursor).

    Returns:
        True if the user exists, False otherwise.
    """
    try:
        # Assumes cursor uses %s style placeholders and returns rowcount
        rowcount = cursor.execute(
            "SELECT `id` FROM user WHERE name = %s LIMIT 1", (username,)
        )
        return rowcount > 0
    except db.Error as e:  # Catch DBAPI errors
        logger.error(
            f"Database error checking if user '{username}' exists: {e}",
            exc_info=True,
        )
        # Depending on desired behavior, might re-raise or return False/None
        return False


def get_modes(cursor: Any) -> Dict[str, Any]:
    """
    Retrieves contact modes from the database using a DBAPI cursor.

    Args:
        cursor: An active DBAPI cursor object (preferably DictCursor).

    Returns:
        A dictionary mapping mode names to their IDs.
    """
    modes = {}
    try:
        cursor.execute("SELECT `name`, `id` FROM `contact_mode`")
        # Assumes DictCursor providing dictionary-like row access
        for row in cursor.fetchall():
            modes[row["name"]] = row["id"]
    except db.Error as e:  # Catch DBAPI errors
        logger.error(
            f"Database error fetching contact modes: {e}", exc_info=True
        )
        # Return empty dict or raise? Returning empty seems safer.
    return modes


def import_user(
    username: str, ldap_contacts: Dict[str, Any], cursor: Any
) -> None:
    """
    Imports a new user and their contacts into the database using a DBAPI cursor.

    Args:
        username: The username to import.
        ldap_contacts: Dictionary of contacts obtained from source (e.g., LDAP).
                       Expected to contain 'full_name'.
        cursor: An active DBAPI cursor object (preferably DictCursor).
    """
    logger.debug("Importing user %s", username)
    # Use .get() for safer access, provide default if needed
    full_name = ldap_contacts.pop(
        "full_name", username
    )  # Default full_name to username if missing
    user_add_sql = "INSERT INTO `user` (`name`, `full_name`, `photo_url`) VALUES (%s, %s, %s)"

    # Get objects needed for insertion
    modes = get_modes(cursor)  # Pass the cursor

    try:
        photo_url_tpl = LDAP_SETTINGS.get("image_url")
        photo_url = photo_url_tpl % username if photo_url_tpl else None

        # Add user
        cursor.execute(user_add_sql, (username, full_name, photo_url))

        # Get newly inserted user ID
        cursor.execute("SELECT `id` FROM user WHERE name = %s", (username,))
        row = cursor.fetchone()
        if not row:
            # This shouldn't happen if the insert succeeded without error, but check anyway
            raise db.Error(
                f"Failed to retrieve user ID after inserting user {username}"
            )
        user_id = row["id"]  # Assumes DictCursor

        stats[
            "users_added"
        ] += 1  # Increment stat only after successful insert+fetch

        # Add contacts
        for key, value in ldap_contacts.items():
            if value and key in modes:
                # Ensure value is string if needed by DB
                if isinstance(value, bytes):
                    try:
                        value = value.decode()
                    except UnicodeDecodeError:
                        logger.warning(
                            f"Could not decode contact {key} for user {username}, skipping."
                        )
                        continue  # Skip if cannot decode
                elif not isinstance(value, str):
                    value = str(value)  # Convert other types to string

                logger.debug("\tAdding contact %s -> %s", key, value)
                user_contact_add_sql = "INSERT INTO `user_contact` (`user_id`, `mode_id`, `destination`) VALUES (%s, %s, %s)"
                # Wrap individual contact inserts in try/except if one failure shouldn't stop others
                try:
                    cursor.execute(
                        user_contact_add_sql, (user_id, modes[key], value)
                    )
                except (
                    db.Error
                ) as contact_e:  # Catch specific contact insert error
                    logger.error(
                        f"Failed to add contact {key} for user {username}: {contact_e}",
                        exc_info=True,
                    )
                    stats["sql_errors"] += 1  # Increment only on actual error

    except db.Error as e:  # Catch DBAPI errors for user insert/fetch
        stats["users_failed_to_add"] += 1
        stats["sql_errors"] += 1
        logger.error(f"Failed to add user {username}: {e}", exc_info=True)
        # Depending on transaction handling, might need rollback here?
        # But commit/rollback should be handled in the calling function (cognito_auth)


def update_user(
    username: str, ldap_contacts: Dict[str, Any], cursor: Any
) -> None:
    """
    Updates an existing user and their contacts in the database using a DBAPI cursor.

    Args:
        username: The username to update.
        ldap_contacts: Dictionary of current contacts obtained from source (e.g., LDAP).
                       Expected to contain 'full_name'.
        cursor: An active DBAPI cursor object (preferably DictCursor).
    """
    logger.debug("Updating user %s", username)
    try:
        # Assume get_oncall_user works with a cursor and returns expected format
        oncall_user = get_oncall_user(username, cursor)
        if not oncall_user or username not in oncall_user:
            logger.error(
                f"Failed to get current DB data for user {username}, cannot update."
            )
            stats["users_failed_to_update"] += 1
            return
        db_contacts = oncall_user[username]

        # Use .get() for safer access, provide default if needed
        full_name = ldap_contacts.pop("full_name", username)

        # Define SQL templates
        contact_update_sql = "UPDATE user_contact SET destination = %s WHERE user_id = (SELECT id FROM user WHERE name = %s) AND mode_id = %s"
        contact_insert_sql = "INSERT INTO user_contact (user_id, mode_id, destination) VALUES ((SELECT id FROM user WHERE name = %s), %s, %s)"
        contact_delete_sql = "DELETE FROM user_contact WHERE user_id = (SELECT id FROM user WHERE name = %s) AND mode_id = %s"
        name_update_sql = "UPDATE user SET full_name = %s WHERE name = %s"
        photo_update_sql = "UPDATE user SET photo_url = %s WHERE name = %s"

        # Get modes
        modes = get_modes(cursor)  # Pass the cursor

        # Update full name if changed
        if full_name != db_contacts.get("full_name"):
            logger.debug(f"\tUpdating full_name to '{full_name}'")
            cursor.execute(name_update_sql, (full_name, username))
            stats["user_names_updated"] += 1

        # Update photo URL if configured and not already set
        if "image_url" in LDAP_SETTINGS and not db_contacts.get("photo_url"):
            photo_url_tpl = LDAP_SETTINGS.get("image_url")
            photo_url = photo_url_tpl % username if photo_url_tpl else None
            if photo_url:  # Only update if we got a URL
                logger.debug(f"\tUpdating photo_url to '{photo_url}'")
                cursor.execute(photo_update_sql, (photo_url, username))
                stats["user_photos_updated"] += 1

        # Update contacts
        processed_ldap_contacts = {}
        # Decode ldap contacts first
        for mode, value in ldap_contacts.items():
            if mode not in modes:
                continue  # Skip modes not in DB

            processed_value = None
            # Handle list values (take first)
            if isinstance(value, list):
                if not value:
                    continue  # Skip empty list
                processed_value = value[0]
            else:
                processed_value = value

            # Decode bytes if necessary
            if isinstance(processed_value, bytes):
                try:
                    processed_value = processed_value.decode()
                except UnicodeDecodeError:
                    logger.warning(
                        f"\tCould not decode contact {mode} for user {username}, skipping update."
                    )
                    continue  # Skip if cannot decode
            elif not isinstance(processed_value, str):
                processed_value = str(
                    processed_value
                )  # Convert other types to string

            processed_ldap_contacts[mode] = processed_value

        # Compare processed LDAP contacts with DB contacts
        all_modes_to_check = set(modes.keys()) | set(db_contacts.keys()) - {
            "full_name",
            "photo_url",
        }  # Consider all relevant modes

        for mode in all_modes_to_check:
            if mode not in modes:
                continue  # Should not happen if modes are fetched correctly

            ldap_value = processed_ldap_contacts.get(mode)
            db_value = db_contacts.get(mode)

            if ldap_value:
                # Contact exists in LDAP source
                if db_value:
                    # Contact exists in DB - check if different
                    if ldap_value != db_value:
                        logger.debug(
                            f"\tUpdating contact {mode} ({db_value} -> {ldap_value})"
                        )
                        cursor.execute(
                            contact_update_sql,
                            (ldap_value, username, modes[mode]),
                        )
                        stats["user_contacts_updated"] += 1
                else:
                    # Contact missing in DB - insert
                    logger.debug(f"\tAdding contact {mode} -> {ldap_value}")
                    cursor.execute(
                        contact_insert_sql, (username, modes[mode], ldap_value)
                    )
                    stats["user_contacts_updated"] += 1
            elif db_value:
                # Contact missing in LDAP source but exists in DB - delete
                logger.debug(f"\tDeleting contact {mode} (was: {db_value})")
                cursor.execute(contact_delete_sql, (username, modes[mode]))
                stats["user_contacts_updated"] += 1
            # else: # Contact missing in both LDAP and DB - do nothing
            #    logger.debug(f"\tMissing contact {mode} in both LDAP and DB")

    except db.Error as e:  # Catch DBAPI errors
        stats["users_failed_to_update"] += 1
        stats["sql_errors"] += 1
        logger.error(f"Failed to update user {username}: {e}", exc_info=True)
        # Rollback should be handled by caller (cognito_auth)
