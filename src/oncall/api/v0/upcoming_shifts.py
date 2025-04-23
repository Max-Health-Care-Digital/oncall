# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import operator
from collections import defaultdict

from ujson import dumps as json_dumps

from ... import db
# Assuming all_columns_select_clause is correctly defined and imported from events.py
from .events import all_columns_select_clause


def on_get(req, resp, user_name):
    """
    Endpoint for retrieving a user's upcoming shifts. Groups linked events into a single
    entity, with the number of events indicated in the ``num_events`` attribute. Non-linked
    events have ``num_events = 0``. Returns a list of event information for each of that
    user's upcoming shifts. Results can be filtered with the query string params below:

    :query limit: The number of shifts to retrieve. Default is unlimited
    :query role: Filters results to return only shifts with the provided roles.

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/users/jdoe/upcoming  HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "end": 1496264400,
                "full_name": "John Doe",
                "id": 169877,
                "link_id": "7b3b96279bb24de8ac3fb7dbf06e5d1e",
                "num_events": 7,
                "role": "primary",
                "schedule_id": 1788,
                "start": 1496221200,
                "team": "team-foo",
                "user": "jdoe"
            }
        ]


    """
    role = req.get_param("role", None)
    limit = req.get_param_as_int("limit")
    query_end = " ORDER BY `event`.`start` ASC"
    query = (
        """SELECT %s
               FROM `event`
               JOIN `user` ON `user`.`id` = `event`.`user_id`
               JOIN `team` ON `team`.`id` = `event`.`team_id`
               JOIN `role` ON `role`.`id` = `event`.`role_id`
               WHERE `user`.`id` = (SELECT `id` FROM `user` WHERE `name` = %%s)
                   AND `event`.`start` > UNIX_TIMESTAMP()"""
        % all_columns_select_clause
    )

    query_params = [user_name]
    if role:
        # Add the role filter to the WHERE clause using a parameter
        query += " AND `role`.`name` = %s"
        query_params.append(role)

    # Add the ordering and potentially limit after the WHERE clause
    query += query_end

    # Add limit to the query if provided (using LIMIT clause)
    if limit is not None:
        # Note: Parameterizing LIMIT is driver-dependent. %s usually works.
        # Alternatively, check DBAPI specifics or use SQLAlchemy ORM.
        # Assuming %s parameter works for LIMIT here.
        query += " LIMIT %s"
        query_params.append(limit)


    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor from the connection wrapper
        cursor = connection.cursor(db.DictCursor)
        # Execute the query with all collected parameters
        cursor.execute(query, query_params)
        data = cursor.fetchall()
        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit cursor.close() and connection.close() are no longer needed.

    # --- Post-processing logic remains outside the with block ---
    # This logic operates on the 'data' list which was fully fetched while
    # the connection was active.
    links = defaultdict(list)
    formatted = []
    for event in data:
        if event["link_id"] is None:
            # Ensure num_events is explicitly set for non-linked events for consistency
            event["num_events"] = 0
            formatted.append(event)
        else:
            links[event["link_id"]].append(event)

    for events in links.values():
        # Find the first event by start time among linked events
        first_event = min(events, key=operator.itemgetter("start"))
        # Copy relevant info from the first event and add num_events
        # Be careful not to modify the list iterated over by links.values() directly
        # if the 'events' list objects are shared. Creating a new dict is safer.
        linked_event_summary = {
            k: v for k, v in first_event.items() if k not in ["mode", "destination", "contact_id"] # Exclude potential raw contact data
        }
        linked_event_summary["num_events"] = len(events)
        formatted.append(linked_event_summary)

    # The initial query included ORDER BY and LIMIT, so sorting/slicing here is
    # redundant if the DB handles it, but kept for safety based on original code.
    # If LIMIT was handled by the DB, this slice might be unnecessary.
    # The sort *might* be needed if the DB's ORDER BY isn't guaranteed across groups.
    # However, fetching all then sorting/limiting can be inefficient for large results.
    # Relying on the database ORDER BY/LIMIT is generally preferred.
    # Removing the Python sort/limit assuming the SQL ORDER BY/LIMIT is sufficient.
    # If the original intent was to fetch more and *then* limit/sort Python-side,
    # the SQL LIMIT should be removed. Let's remove the redundant Python steps.

    # formatted = sorted(formatted, key=operator.itemgetter("start")) # Removed redundant sort
    # if limit is not None: # Removed redundant limit
    #     formatted = formatted[:limit]

    resp.text = json_dumps(formatted)