# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# Assuming necessary imports are handled by __init__.py or similar for falcon, db, etc.
# from falcon import HTTP_200 # Implicit 200 success if no other status set
# from ujson import dumps # Not used in this specific function

from .. import db


def on_post(req, resp):
    """
    Endpoint to delete a user's session.

    **Example request:**

    .. sourcecode:: http

       POST /api/v0/logout HTTP/1.1
       Host: example.com

    :statuscode 200: Successful logout
    """
    # Assuming req.env["beaker.session"] is provided by Beaker middleware
    session = req.env["beaker.session"]

    # Use the 'with' statement for safe connection and transaction management
    with db.connect() as connection:
        cursor = connection.cursor()

        try:
            # Execute the DELETE query using parameterized session ID
            # Assuming session["_id"] is the correct identifier
            cursor.execute(
                "DELETE FROM `session` WHERE `id` = %s", (session["_id"],)
            )

            # Commit the transaction if the delete succeeds
            # The try block implicitly starts here. Exceptions trigger rollback via 'with'.
            connection.commit()

        except Exception as e:  # Catch any exceptions during the DB transaction
            # The with statement handles rollback automatically if an exception is raised within the block before commit.
            print(
                f"Error deleting session ID {session['_id']}: {e}"
            )  # Replace with logging
            # Re-raise the exception (e.g., for Falcon to translate to 500 Internal Server Error)
            raise

        # Do not need finally block; rely on the 'with' statement for close.

    # Delete the session using the Beaker middleware's session object
    session.delete()

    # Default Falcon response status is 200 OK if not explicitly set
    # resp.status = HTTP_200 # Optional: explicitly set 200 status
