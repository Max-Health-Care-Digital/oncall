# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import db

filters = {
    # These filters use dictionary-style placeholders (%(name)s)
    "owner": "`owner_name` = %(owner)s",
    "team": "`team_name` = %(team)s",
    # For the 'action' filter, the value passed via params will be a list/tuple,
    # and the DBAPI driver will correctly format the IN clause.
    "action": "`action_name` IN %(action)s",
    "start": "`timestamp` >= %(start)s",
    "end": "`timestamp` <= %(end)s",
    # Assuming the 'id' query param from the docstring is for audit.id
    "id": "`audit`.`id` = %(id)s",
    "id__eq": "`audit`.`id` = %(id__eq)s",
    # Add other potential constraints if needed based on actual usage
}


def on_get(req, resp):
    """
    Search audit log. Allows filtering based on a number of parameters,
    detailed below. Returns an entry in the audit log, including the name
    of the associated team, action owner, and action type, as well as a
    timestamp and the action context. The context tracks different data
    based on the action, which may be useful in investigating.
    Audit logs are tracked for the following actions:

    * admin_created
    * event_created
    * event_edited
    * roster_created
    * roster_edited
    * roster_user_added
    * roster_user_deleted
    * team_created
    * team_edited
    * event_deleted
    * event_swapped
    * roster_user_edited
    * team_deleted
    * admin_deleted
    * roster_deleted
    * event_substituted


    **Example request**:

    .. sourcecode:: http

       GET /api/v0/audit?team=foo-sre&end=1487466146&action=event_created  HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "context":"{"new_event_id":441072,"request_body":{"start":1518422400,"end":1518595200,"role":"primary","user":jdoe","team":"foo-sre"}}"
                "timestamp": 1488441600,
                "team_name": "foo-sre",
                "owner_name": "jdoe"
                "action_name: "event_created"
            }
        ]

    :query team: team name
    :query owner: action owner name
    :query action: name of action taken. If provided multiple action names,
    :query id: id of the event (assuming this means audit entry id)
    :query start: lower bound for audit entry's timestamp (unix timestamp)
    :query end: upper bound for audit entry's timestamp (unix timestamp)
    """
    # Preprocess parameters as needed before building the query or executing
    request_params = req.params.copy() # Work on a copy to avoid modifying req.params directly
    if "action" in request_params:
        # Ensure action is a list/tuple for the IN clause
        action_value = request_params["action"]
        if not isinstance(action_value, (list, tuple)):
             request_params["action"] = [action_value] # Wrap single value in a list
        # If it's already a list/tuple, use it as is.


    query = """SELECT `owner_name` AS `owner`, `team_name` AS `team`,
                   `action_name` AS `action`, `timestamp`, `context`
               FROM `audit`"""

    # Build WHERE clause using dictionary-style constraints
    where_params_snippets = []
    # Iterate through request_params keys, checking against filters
    for field in request_params.keys():
        if field in filters:
            where_params_snippets.append(filters[field])
        # else: Ignore unknown parameters

    where_clause = " AND ".join(where_params_snippets) if where_params_snippets else "1" # Use "1" for no WHERE conditions

    # Combine query template and WHERE clause
    # Note: Using dictionary parameters %(name)s means the query string
    # is fully constructed here, but the values are passed separately
    # to cursor.execute. This is correct and safe.
    if where_clause != "1":
        query = f"{query} WHERE {where_clause}"


    # Add ordering for consistent results (optional but good practice)
    # query += " ORDER BY `timestamp` DESC" # Example ordering


    # Use the 'with' statement for safe connection management
    with db.connect() as connection:
        # Acquire a dictionary cursor
        cursor = connection.cursor(db.DictCursor)

        # Execute the query using the constructed query string and the request_params dictionary
        cursor.execute(query, request_params)

        # Fetch all results
        results = cursor.fetchall()

        # The connection and cursor will be automatically closed/released
        # when the 'with' block exits, even if an error occurs.
        # Explicit close calls are no longer needed.

    # Continue processing outside the with block using the fetched 'results' list
    resp.text = json_dumps(results)