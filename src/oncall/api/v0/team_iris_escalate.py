# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from typing import Any, Dict

from falcon import HTTP_200, HTTPBadRequest
from requests import (  # requests.HTTPError used here, different from falcon.HTTPError
    ConnectionError,
    HTTPError,
)

from ... import db, iris
from ...auth import login_required
from ...constants import CUSTOM, MEDIUM, URGENT
from ...utils import load_json_body


@login_required
def on_post(req, resp, team):
    """
    Escalate to a team using Iris. Configured in the 'iris_plan_integration' section of
    the configuration file. Escalation plan is specified via keyword, currently: 'urgent',
    'medium', or 'custom'. These keywords correspond to the plan specified in the
    iris_plan_integration urgent_plan key, the iris integration medium_plan key, and the team's
    iris plan defined in the DB, respectively. If no plan is specified, the team's custom plan will be
    used. If iris plan integration is not activated, this endpoint will be disabled.

    **Example request:**

    .. sourcecode:: http

        POST /v0/events   HTTP/1.1
        Content-Type: application/json

        {
            "description": "Something bad happened!",
            "plan": "urgent"
        }

    :statuscode 200: Incident created
    :statuscode 400: Escalation failed, missing description/No escalation plan specified
    for team/Iris client error.
    """
    data = load_json_body(req)

    plan = data.get("plan")
    dynamic = False
    plan_name = None  # Initialize plan_name outside the conditional blocks

    plan_settings: Dict[str, Any] = {}
    if plan == URGENT:
        plan_settings = iris.settings["urgent_plan"]
        dynamic = True
        plan_name = plan_settings["name"]  # Assign plan_name directly
    elif plan == MEDIUM:
        plan_settings = iris.settings["medium_plan"]
        dynamic = True
        plan_name = plan_settings["name"]  # Assign plan_name directly
    elif plan == CUSTOM or plan is None:
        # Default to team's custom plan for backwards compatibility
        # *** Use the 'with' statement for safe database interaction ***
        with db.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT iris_plan FROM team WHERE name = %s", (team,)
            )  # Parameterize team name as tuple

            # Check if team exists and has a custom plan
            # Fetchone here to get the result if found
            row = cursor.fetchone()

            # Check if the team was found AND has a non-None iris_plan
            if not row or row[0] is None:
                # The connection and cursor are automatically closed/released by the 'with' block
                raise HTTPBadRequest(
                    "Iris escalation failed",
                    f"Team '{team}' not found or has no custom escalation plan defined",
                )

            plan_name = row[0]  # Assign the fetched plan name

        # The connection and cursor are automatically closed/released by the 'with' block when it exits
        # Explicit close calls are no longer needed here.

    else:
        raise HTTPBadRequest(
            "Iris escalation failed", "Invalid escalation plan"
        )

    # Rest of the logic remains outside the DB connection management
    requester = req.context.get("user")
    if not requester:
        requester = req.context[
            "app"
        ]  # Assuming 'app' key exists in req.context for app identity
    data["requester"] = requester

    if "description" not in data or data["description"] == "":
        raise HTTPBadRequest(
            "Iris escalation failed",
            "Escalation cannot have an empty description",
        )

    # Validate Iris plan name is set (should be guaranteed by previous blocks)
    if not plan_name:
        raise HTTPError(
            "500 Internal Server Error",
            "Configuration Error",
            "Escalation plan name was not determined correctly.",
        )

    try:
        if dynamic:
            # Dynamic plan settings are already determined
            targets = plan_settings["dynamic_targets"]
            for t in targets:
                # Set target to team name if not overridden in settings
                if "target" not in t:
                    t["target"] = team
            # Interact with external Iris client - NOT a DB operation
            re = iris.client.post(
                iris.client.url + "incidents",
                json={
                    "plan": plan_name,  # Use determined plan_name
                    "context": data,
                    "dynamic_targets": targets,
                },
            )
            re.raise_for_status()
            incident_id = re.json()
        else:
            # Interact with external Iris client - NOT a DB operation
            # Use the determined plan_name for custom/default plans
            # Ensure context data format is suitable for iris.client.incident
            incident_id = iris.client.incident(plan_name, context=data)

    except (
        ValueError,
        ConnectionError,
        HTTPError,
    ) as e:  # Catch exceptions from the requests library or Iris client
        # Re-raise as Falcon HTTPBadRequest with a specific error message
        raise HTTPBadRequest(
            "Iris escalation failed", f"Iris client error: {e}"
        ) from e  # Include original exception for traceback

    # Set the response text with the incident ID
    resp.text = str(incident_id)
    resp.status = HTTP_200  # Standard response for successful creation/action
