import logging
import time # time imported but not used?

from gevent import sleep # Assuming gevent is used for sleep

from oncall import db, messengers # Assuming messengers is imported

logger = logging.getLogger(__name__)

# Assuming HOUR, DAY, WEEK constants are defined elsewhere if needed

def user_validator(config):
    """
    Validates users with future events have a call contact. Logs and sends messages for invalid users.
    Runs in a periodic loop.
    """
    subject = config["subject"]
    body = config["body"]
    sleep_time = config.get("interval", 86400) # Use .get with default

    # The query is static, define it once outside the loop
    query = """SELECT `user`.`name`
                          FROM `event` LEFT JOIN `user_contact` ON `event`.`user_id` = `user_contact`.`user_id`
                              AND `user_contact`.`mode_id` = (SELECT `id` FROM `contact_mode` WHERE `name` = 'call')
                          JOIN `user` ON `event`.`user_id` = `user`.`id`
                          WHERE `event`.`start` > UNIX_TIMESTAMP() AND `user_contact`.`destination` IS NULL
                          GROUP BY `event`.`user_id`;"""

    while 1:
        # Sleep first so bouncing notifier doesn't spam
        logger.info("User validator sleeping for %s seconds...", sleep_time)
        sleep(sleep_time)

        logger.info("User validator polling started.")

        # Use the 'with' statement for safe database interaction within each loop iteration
        try:
            with db.connect() as connection:
                cursor = connection.cursor() # Use standard cursor

                # Execute the query (no parameters needed)
                cursor.execute(query)

                # Process results while the connection is active
                # Iterate through results and send messages for invalid users
                for row in cursor:
                    # row[0] is the user name based on the SELECT statement
                    user_name = row[0]

                    logger.warning(f"User '{user_name}' has upcoming events but no call contact.")

                    message = {
                        "user": user_name,
                        "mode": "email", # Assuming sending via email mode
                        "subject": subject,
                        "body": body,
                    }
                    # Send message via messengers module (not a DB operation using db.connect())
                    # Assuming messengers.send_message handles its own connections or uses a pool
                    try:
                         messengers.send_message(message)
                         logger.info(f"Sent validation message to user '{user_name}'.")
                    except Exception as send_e:
                         logger.error(f"Error sending validation message to user '{user_name}': {send_e}")


                # The connection and cursor will be automatically closed/released
                # when the 'with' block exits (after the loop finishes or an exception occurs).

        except Exception as e: # Catch any exceptions during DB interaction or processing results
            # The with statement handles rollback automatically (though it's a SELECT, so no commit needed).
            logger.error(f"Error during user validation polling: {e}")
            # Decide how to handle errors: continue to next loop after logging, or stop?
            # For a background worker, continuing is often desired.
            # A sleep inside the except might prevent tight looping on persistent errors.
            sleep(10) # Sleep briefly on error

        logger.info("User validator polling finished.")