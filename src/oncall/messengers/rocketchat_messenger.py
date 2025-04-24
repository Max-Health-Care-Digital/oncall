import time

import requests

from oncall import db
from oncall.constants import ROCKET_SUPPORT


class rocketchat_messenger(object):
    supports = frozenset([ROCKET_SUPPORT])

    def __init__(self, config):
        self.user = config["user"]
        self.password = config["password"]
        self.api_host = config["api_host"]
        self.refresh = config.get("refresh", 2592000)  # default 30 days
        self.last_auth = None
        self.token = None
        self.user_id = None
        self.authenticate()

    def authenticate(self):
        self.last_auth = time.time()
        re = requests.post(
            self.api_host + "/api/v1/login",
            json={"username": self.user, "password": self.password},
        )
        data = re.json()
        if re.status_code != 200 or data["status"] != "success":
            raise ValueError("Invalid RocketChat credentials")
        self.token = data["data"]["authToken"]
        self.user_id = data["data"]["userId"]

    def send(self, message):
        """
        Sends a message to a user's Rocketchat destination.
        """
        # Perform external authentication check if needed
        if self.last_auth is None or (time.time() - self.last_auth > self.refresh):
            self.authenticate()

        # Use the 'with' statement for safe database interaction
        # Wrap the DB interaction and the ValueError check that follows
        try:
            with db.connect() as connection:
                cursor = connection.cursor()  # Use standard cursor

                # Execute the query to find the Rocketchat destination
                cursor.execute(
                    """SELECT `destination` FROM `user_contact`
                                WHERE `user_id` = (SELECT `id` FROM `user` WHERE `name` = %s)
                                AND `mode_id` = (SELECT `id` FROM `contact_mode` WHERE `name` = 'rocketchat')""",
                    (message["user"],),  # Parameterize user name as a tuple
                )

                # Fetch the single result
                target_row = cursor.fetchone()

                # Check if a destination was found
                if not target_row:  # fetchone returns None if no rows found
                    # Raise ValueError within the with block
                    # The context manager handles connection cleanup before the exception propagates.
                    raise ValueError(
                        f"Rocketchat username not found for {message.get('user', 'N/A')}"  # Use .get for safety
                    )

                target = target_row[0]  # Get the destination value

                # The connection and cursor are automatically closed/released
                # when the 'with' block exits.

        except ValueError as e:  # Catch the specific ValueError raised above
            # Re-raise the ValueError for the caller of the send method
            raise e
        except (
            Exception
        ) as e:  # Catch any other unexpected exceptions during DB interaction
            # The with statement handles rollback automatically.
            print(
                f"Error looking up Rocketchat destination for user {message.get('user', 'N/A')}: {e}"
            )  # Replace with logging
            # Re-raise the exception for the caller
            raise

        # Interact with the external Rocketchat API - NOT a DB operation
        # This happens *after* the DB lookup is complete and the connection is closed.
        try:
            re = requests.post(
                self.api_host + "/api/v1/chat.postMessage",
                json={
                    "channel": "@%s"
                    % target,  # Using string formatting here - ensure 'target' is safe or encode it if needed
                    "text": " -- ".join(
                        [message.get("subject", ""), message.get("body", "")]
                    ),  # Use .get for message fields safety
                },
                headers={"X-User-Id": self.user_id, "X-Auth-Token": self.token},
            )
            re.raise_for_status()  # Raise an HTTPError for bad responses

            # Check the 'success' key in the JSON response
            response_json = re.json()
            if not response_json.get("success"):  # Use .get for safety
                raise ValueError(
                    f"Rocketchat API returned success=false: {response_json.get('error', 'No error detail provided')}"
                )  # Include error detail if available

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            ValueError,
        ) as e:  # Catch specific requests exceptions and our custom ValueError
            # Re-raise as a ValueError indicating failure to contact Rocketchat
            # The original message was "Failed to contact rocketchat". Let's keep that or enhance.
            raise ValueError(
                f"Failed to send message to Rocketchat: {e}"
            ) from e  # Include original exception

        # If successful, the function completes without returning anything (implicit None return)
