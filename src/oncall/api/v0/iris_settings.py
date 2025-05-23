# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from ujson import dumps as json_dumps

from ... import iris


def on_get(req, resp):
    if iris.settings is None:
        resp.text = json_dumps({"activated": False})
    else:
        resp.text = json_dumps(iris.settings)
