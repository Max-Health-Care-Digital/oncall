# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time

from falcon import HTTPNotFound

from . import ical
from .ical_key import get_name_and_type_from_key
from .team_ical import get_team_events
from .user_ical import get_user_events

allow_no_auth = True


def on_get(req, resp, key):
    """Get ical file for a user or team's oncall calendar with no contact
    information.  Key can be requested at /api/v0/ical_key.

    """
    roles = req.get_param_as_list("roles")
    excluded_teams = req.get_param_as_list("excludedTeams")

    name_and_type = get_name_and_type_from_key(key)
    if name_and_type is None:
        raise HTTPNotFound()

    name, type = name_and_type
    start = int(time.time())
    events = []
    if type == "user":
        events = get_user_events(
            name, start, roles=roles, excluded_teams=excluded_teams
        )
    elif type == "team":
        events = get_team_events(
            name, start, roles=roles, include_subscribed=True
        )

    resp.text = ical.events_to_ical(events, name, contact=False)
    resp.set_header("Content-Type", "text/calendar")
