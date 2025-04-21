# Import necessary libraries
import base64
import hashlib
import hmac
import logging
import sys

import boto3  # Added for AWS Cognito
from botocore.exceptions import ClientError  # Added for Boto3 error handling
from sqlalchemy.exc import SQLAlchemyError

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

        from oncall import db

        self.connection = db.connect()
        self.engine = self.connection.cursor(db.DictCursor)

        self.aws_region = config.get("aws_region")
        self.user_pool_id = config.get("cognito_user_pool_id")
        self.app_client_id = config.get("cognito_app_client_id")
        self.app_client_secret = config.get("cognito_app_client_secret")

        self.import_user = config.get("import_user", False)

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
                logger.info(
                    f"Cognito authentication successful for user: {username}"
                )

                if self.import_user:
                    from oncall.user_sync.ldap_sync import (
                        import_user,
                        update_user,
                        user_exists,
                    )

                    if user_exists(username, self.engine):
                        logger.info(
                            "user %s already exists, updating from ldap",
                            username,
                        )
                        update_user(username, ldap_contacts, self.engine)
                    else:
                        logger.info(
                            "user %s does not exists. importing.", username
                        )
                        import_user(username, ldap_contacts, self.engine)
                    self.connection.commit()
                    self.engine.close()
                    self.connection.close()

                return True
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

    def user_exists(self, username):
        return self.engine.execute(
            "SELECT `id` FROM user WHERE name = %s", username
        )

    def import_user(self, username, contact):
        logger.debug("Inserting %s" % username)
        # full_name = ldap_contacts.pop('full_name')
        user_add_sql = "INSERT INTO `user` (`name`, `full_name`, `photo_url`) VALUES (%s, %s, %s)"

        # get objects needed for insertion
        modes = self.get_modes()

        try:
            # photo_url_tpl = LDAP_SETTINGS.get('image_url')
            photo_url = ""
            self.engine.execute(user_add_sql, (username, username, photo_url))
            self.engine.execute(
                "SELECT `id` FROM user WHERE name = %s", username
            )
            row = self.engine.fetchone()
            user_id = row["id"]
        except SQLAlchemyError:
            logger.exception("Failed to add user %s" % username)
            return

        for key, value in contact.items():
            if value and key in modes:
                logger.debug("\t%s -> %s" % (key, value))
                user_contact_add_sql = "INSERT INTO `user_contact` (`user_id`, `mode_id`, `destination`) VALUES (%s, %s, %s)"
                self.engine.execute(
                    user_contact_add_sql, (user_id, modes[key], value)
                )

    def get_modes(
        self,
    ):
        self.engine.execute("SELECT `name`, `id` FROM `contact_mode`")
        modes = {}
        for row in self.engine.fetchall():
            modes[row["name"]] = row["id"]
        return modes
