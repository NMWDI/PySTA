# ===============================================================================
# Copyright 2021 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
from urllib.parse import quote_plus

from requests import Session
from jsonschema import validate, ValidationError
import re

IDREGEX = re.compile(r"(?P<id>\(\d+\))")


class BaseST:
    iotid = None
    _db_obj = None

    def __init__(self, payload, session, connection):
        self._payload = payload
        self._connection = connection
        self._session = session

    def _validate_payload(self):
        try:
            validate(instance=self._payload, schema=self._schema)
            return True
        except ValidationError as err:
            print(
                f"Validation failed for {self.__class__.__name__}. {err}. {self._payload}"
            )

    def _generate_request(self, method, query=None, entity=None):
        base_url = self._connection["base_url"]
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}/FROST-Server/v1.1"

        if entity is None:
            entity = self.__class__.__name__

        url = f"{base_url}/{entity}"
        if method == "patch":
            url = f"{url}({self.iotid})"
        else:
            if query:
                url = f"{url}?$filter={quote_plus(query)}"

        return {"method": method, "url": url}

    def _send_request(self, request, **kw):
        connection = self._connection
        func = getattr(self._session, request["method"])
        return func(request["url"], auth=(connection["user"], connection["pwd"]), **kw)

    def _parse_response(self, request, resp):
        if request["method"] == "get":
            if resp.status_code == 200:
                return resp.json()
        elif request["method"] == "post":
            if resp.status_code == 201:
                m = IDREGEX.search(resp.headers.get("location", ""))
                if m:
                    iotid = m.group("id")[1:-1]
                    self.iotid = iotid
                    return True
        elif request["method"] == "patch":
            if resp.status_code == 200:
                return True

    def get(self, query, entity=None):
        request = self._generate_request("get", query=query, entity=entity)
        resp = self._send_request(request)
        resp = self._parse_response(request, resp)
        if resp:
            return resp["value"]

    def put(self):
        if self._validate_payload():
            if self.exists():
                return self.patch()
            else:
                request = self._generate_request("post")
                resp = self._send_request(request, json=self._payload)
                return self._parse_response(request, resp)

    def exists(self):
        name = self._payload["name"]
        resp = self.get(f"name eq '{name}'")
        if resp:
            try:
                self._db_obj = resp["value"][0]
            except IndexError:
                return

            self.iotid = self._db_obj["@iot.id"]
            return True

    def patch(self):
        if self._validate_payload():
            request = self._generate_request("patch")
            resp = self._send_request(request, json=self._payload)
            return self._parse_response(request, resp)


class Things(BaseST):
    _schema = {
        "type": "object",
        "required": ["name", "description"],
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "Locations": {
                "type": "array",
                "required": ["@iot.id"],
                "properties": {"@iot.id": {"type": "number"}},
            },
        },
    }

    def exists(self):
        name = self._payload["name"]
        location = self._payload["Locations"][0]
        lid = location["@iot.id"]
        resp = self.get(f"name eq '{name}'", entity=f"Locations({lid})/Things")

        if resp:
            try:
                self._db_obj = resp[0]
            except IndexError:
                return

            self.iotid = self._db_obj["@iot.id"]
            return True


class Locations(BaseST):
    _schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "encodingType": {"type": "string"},
            "location": {
                "type": "object",
                "required": ["type", "coordinates"],
                "properties": {
                    "type": {"type": "string"},
                    "coordinates": {"type": "array", "items": {"type": "number"}},
                },
            },
            "properties": {"type": "object"},
        },
        "required": ["name", "description", "encodingType", "location"],
    }


class Client:
    def __init__(self, base_url, user, pwd):
        self._connection = {"base_url": base_url, "user": user, "pwd": pwd}
        self._session = Session()

    def put_location(self, payload):
        location = Locations(payload, self._session, self._connection)
        location.put()
        return location

    def put_thing(self, payload):
        thing = Things(payload, self._session, self._connection)
        thing.put()
        return thing

    def get_locations(self, query=None):
        yield from Locations(None, self._session, self._connection).get(query)

    def get_things(self, query=None):
        yield from Things(None, self._session, self._connection).get(query)

    def get_location(self, query=None):
        return next(self.get_locations(query))

    def get_thing(self, query=None):
        return next(self.get_locations(query))


# ============= EOF =============================================
