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
import os.path

import click
import yaml
from requests import Session
from jsonschema import validate, ValidationError
import re

IDREGEX = re.compile(r"(?P<id>\(\d+\))")


def verbose_message(msg):
    click.secho(msg, fg="green")


def warning(msg):
    click.secho(msg, fg="red")


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

    def _generate_request(
        self, method, query=None, entity=None, orderby=None, expand=None, limit=None
    ):
        if orderby is None and method == "get":
            orderby = "$orderby=id asc"

        base_url = self._connection["base_url"]
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}/FROST-Server/v1.1"

        if entity is None:
            entity = self.__class__.__name__

        url = f"{base_url}/{entity}"
        if method == "patch":
            url = f"{url}({self.iotid})"
        else:
            params = []
            if limit:
                params.append(f"$top={limit}")

            if orderby:
                if not orderby.startswith("$orderby"):
                    orderby = f"$orderby={orderby}"
                params.append(orderby)

            if query:
                # params.append(urlencode({"$filter": query}))

                params.append(f"$filter={query}")

            if params:
                url = f"{url}?{'&'.join(params)}"
            if expand:
                url = f"{url}&$expand={expand}"

        return {"method": method, "url": url}

    def _send_request(self, request, dry=False, verbose=True, **kw):
        connection = self._connection
        func = getattr(self._session, request["method"])
        if not dry:
            resp = func(
                request["url"], auth=(connection["user"], connection["pwd"]), **kw
            )
            if verbose:
                if resp and resp.status_code not in (200, 201):
                    print(f"request={request}")
                    print(f"response={resp}")
            return resp

    def _parse_response(self, request, resp, dry=False):
        if request["method"] == "get":
            if resp.status_code == 200:
                return resp.json()
        elif request["method"] == "post":

            if dry:
                return True

            if resp.status_code == 201:
                m = IDREGEX.search(resp.headers.get("location", ""))
                if m:
                    iotid = m.group("id")[1:-1]
                    self.iotid = iotid
                    return True
            else:
                print(resp.status_code, resp.text)

        elif request["method"] == "patch":
            if resp.status_code == 200:
                return True

    def get(
        self,
        query,
        entity=None,
        pages=None,
        expand=None,
        limit=None,
        verbose=False,
        orderby=None,
    ):

        if pages and pages < 0:
            pages = abs(pages)
            orderby = "$orderby=id desc"

        def get_items(request, page_count, yielded):
            if pages:
                if page_count >= pages:
                    return

            if verbose:
                pv = ""
                if pages:
                    pv = "/{pages}"

                verbose_message(
                    f"getting page={page_count + 1}{pv} - url={request['url']}"
                )
                # verbose_message("-------------- Request -----------------")
                # verbose_message(request["url"])
                # verbose_message("----------------------------------------")

            resp = self._send_request(request)
            resp = self._parse_response(request, resp)
            if not resp:
                click.secho(request["url"], fg="red")
                return

            if not resp["value"]:
                warning("no records found")
                return
            else:
                for v in resp["value"]:
                    if limit and yielded >= limit:
                        return

                    yielded += 1
                    yield v
                try:
                    next_url = resp["@iot.nextLink"]
                except KeyError:
                    return

            yield from get_items(
                {"method": "get", "url": next_url}, page_count + 1, yielded
            )

        start_request = self._generate_request(
            "get",
            query=query,
            entity=entity,
            orderby=orderby,
            expand=expand,
            limit=limit,
        )
        yield from get_items(start_request, 0, 0)

    def put(self, dry=False, check_exists=True):
        if self._validate_payload():
            if check_exists and self.exists():
                return self.patch()
            else:
                request = self._generate_request("post")
                print(request)
                resp = self._send_request(request, json=self._payload, dry=dry)

                return self._parse_response(request, resp, dry=dry)

    def getfirst(self, *args, **kw):
        try:
            return next(self.get(*args, **kw))
        except StopIteration:
            return

    def exists(self):
        name = self._payload["name"]
        resp = self.getfirst(f"name eq '{name}'")
        if resp:
            try:
                self._db_obj = resp
            except IndexError:
                return

            self.iotid = self._db_obj["@iot.id"]
            return True

    def patch(self, dry=False):
        if self._validate_payload():
            request = self._generate_request("patch")
            resp = self._send_request(request, json=self._payload, dry=dry)
            return self._parse_response(request, resp, dry=dry)


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
        resp = self.getfirst(f"name eq '{name}'", entity=f"Locations({lid})/Things")

        if resp:
            try:
                self._db_obj = resp
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
                "oneOf": [
                    {
                        "title": "Point",
                        "type": "object",
                        "properties": {
                            "type": {"enum": ["Point"]},
                            "coordinates": {"$ref": "#/definitions/position"},
                        },
                    },
                    {
                        "title": "Polygon",
                        "type": "object",
                        "properties": {
                            "type": {"enum": ["Polygon"]},
                            "coordinates": {"$ref": "#/definitions/polygon"},
                        },
                    },
                ],
            },
        },
        "required": ["name", "description", "encodingType", "location"],
        "definitions": {
            "position": {
                "description": "A single position",
                "type": "array",
                "minItems": 2,
                "items": {"type": "number"},
                "additionalItems": False,
            },
            "positionArray": {
                "description": "An array of positions",
                "type": "array",
                "items": {"$ref": "#/definitions/position"},
            },
            "lineString": {
                "description": "An array of two or more positions",
                "allOf": [{"$ref": "#/definitions/positionArray"}, {"minItems": 2}],
            },
            "linearRing": {
                "description": "An array of four positions where the first equals the last",
                "allOf": [{"$ref": "#/definitions/positionArray"}, {"minItems": 4}],
            },
            "polygon": {
                "description": "An array of linear rings",
                "type": "array",
                "items": {"$ref": "#/definitions/linearRing"},
            },
        },
    }


class Sensors(BaseST):
    _schema = {
        "type": "object",
        "required": ["name", "description", "encodingType", "metadata"],
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "encodingType": {"type": "string"},
            "metadata": {"type": "string"},
        },
    }


class ObservedProperties(BaseST):
    _schema = {
        "type": "object",
        "required": ["name", "definition", "description"],
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "definition": {"type": "string"},
        },
    }


class Datastreams(BaseST):
    _schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "unitOfMeasurement": {
                "type": "object",
                "required": ["name", "symbol", "definition"],
                "properties": {
                    "name": {"type": "string"},
                    "symbol": {"type": "string"},
                    "definition": {"type": "string"},
                },
            },
            "observationType": {"type": "string"},
            "Thing": {
                "type": "object",
                "required": ["@iot.id"],
                "properties": {"@iot.id": {"type": "number"}},
            },
            "ObservedProperty": {
                "type": "object",
                "required": ["@iot.id"],
                "properties": {"@iot.id": {"type": "number"}},
            },
            "Sensor": {
                "type": "object",
                "required": ["@iot.id"],
                "properties": {"@iot.id": {"type": "number"}},
            },
        },
        "required": [
            "name",
            "description",
            "unitOfMeasurement",
            "observationType",
            "Thing",
            "ObservedProperty",
            "Sensor",
        ],
    }

    def exists(self):
        name = self._payload["name"]
        thing = self._payload["Thing"]
        lid = thing["@iot.id"]
        resp = self.getfirst(f"name eq '{name}'", entity=f"Things({lid})/Datastreams")

        if resp:
            try:
                self._db_obj = resp
            except IndexError:
                return

            self.iotid = self._db_obj["@iot.id"]
            return True


class Observations(BaseST):
    _schema = {
        "type": "object",
        "required": ["phenomenonTime", "result", "resultTime", "Datastream"],
        "properties": {
            "phenomenonTime": {"type": "string"},
            "result": {"type": "number"},
            "resultTime": {"type": "string"},
            "Datastream": {
                "type": "object",
                "required": ["@iot.id"],
                "properties": {"@iot.id": {"type": "number"}},
            },
        },
    }


class ObservationsArray(BaseST):
    _schema = {
        "type": "object",
        "required": ["observations", "Datastream", "components"],
        "properties": {
            "observations": {"type": "array"},
            "components": {"type": "array"},
            "Datastream": {
                "type": "object",
                "required": ["@iot.id"],
                "properties": {"@iot.id": {"type": "number"}},
            },
        },
    }

    def put(self, dry=False):
        if self._validate_payload():
            obs = self._payload["observations"]
            n = 100
            nobs = len(obs)
            for i in range(0, nobs, n):
                print("loading chunk {}/{}".format(i, nobs))
                chunk = obs[i : i + n]

                pd = [
                    {
                        "Datastream": self._payload["Datastream"],
                        "components": self._payload["components"],
                        "dataArray": chunk,
                    }
                ]
                base_url = self._connection["base_url"]
                if not base_url.startswith("http"):
                    base_url = f"https://{base_url}/FROST-Server/v1.1"

                url = f"{base_url}/CreateObservations"
                request = {"method": "post", "url": url}
                resp = self._send_request(request, json=pd, dry=dry)

                self._parse_response(request, resp, dry=dry)


class Client:
    def __init__(self, base_url=None, user=None, pwd=None):
        self._connection = {"base_url": base_url, "user": user, "pwd": pwd}
        if not base_url:
            p = os.path.join(os.path.expanduser("~"), ".sta.yaml")
            if os.path.isfile(p):
                with open(p, "r") as rfile:
                    obj = yaml.load(rfile, Loader=yaml.SafeLoader)
                    self._connection.update(**obj)

        if not self._connection["base_url"]:
            base_url = input("Please enter a base url for a SensorThings instance>> ")
            if base_url.endswith("/"):
                base_url = base_url[:-1]
            self._connection["base_url"] = base_url
            with open(p, "w") as wfile:
                yaml.dump(self._connection, wfile)

        self._session = Session()

    @property
    def base_url(self):
        return self._connection["base_url"]

    def put_sensor(self, payload, dry=False):
        sensor = Sensors(payload, self._session, self._connection)
        sensor.put(dry)
        return sensor

    def put_observed_property(self, payload, dry=False):
        obs = ObservedProperties(payload, self._session, self._connection)
        obs.put(dry)
        return obs

    def put_datastream(self, payload, dry=False):
        datastream = Datastreams(payload, self._session, self._connection)
        datastream.put(dry)
        return datastream

    def put_location(self, payload, dry=False):
        location = Locations(payload, self._session, self._connection)
        location.put(dry)
        return location

    def put_thing(self, payload, dry=False):
        thing = Things(payload, self._session, self._connection)
        thing.put(dry)
        return thing

    def add_observations(self, payload, dry=False):
        obs = ObservationsArray(payload, self._session, self._connection)
        obs.put(dry)
        return obs

    def add_observation(self, payload, dry=False):
        obs = Observations(payload, self._session, self._connection)
        obs.put(dry, check_exists=False)
        return obs

    def patch_location(self, iotid, payload, dry=False):
        location = Locations(payload, self._session, self._connection)
        location.patch(dry)
        return location

    def get_sensors(self, query=None, name=None):
        if name is not None:
            query = f"name eq '{name}'"

        yield from Sensors(None, self._session, self._connection).get(query)

    def get_observed_properties(self, query=None, name=None):
        if name is not None:
            query = f"name eq '{name}'"
        yield from ObservedProperties(None, self._session, self._connection).get(query)

    def get_datastreams(self, query=None, **kw):
        yield from Datastreams(None, self._session, self._connection).get(query, **kw)

    def get_locations(self, query=None, **kw):
        yield from Locations(None, self._session, self._connection).get(query, **kw)

    def get_things(self, query=None, **kw):
        yield from Things(None, self._session, self._connection).get(query, **kw)

    def get_location(self, query=None, name=None):
        if name is not None:
            query = f"name eq '{name}'"
        try:
            return next(self.get_locations(query))
        except StopIteration:
            pass

    def get_thing(self, query=None, name=None, location=None):
        entity = None
        if location:
            if isinstance(location, dict):
                location = location["@iot.id"]
            entity = "Locations({})/Things".format(location)
        if name is not None:
            query = f"name eq '{name}'"

        return next(self.get_things(query, entity=entity))

    def get_datastream(self, query=None, name=None, thing=None):

        entity = None
        if thing:
            if isinstance(thing, dict):
                thing = thing["@iot.id"]
            entity = f"Things({thing})/Datastreams"
        if name is not None:
            query = f"name eq '{name}'"

        return next(self.get_datastreams(query, entity=entity))

    def get_observations(self, datastream, **kw):
        if isinstance(datastream, dict):
            datastream = datastream["@iot.id"]
        entity = f"Datastreams({datastream})/Observations"

        yield from Datastreams(None, self._session, self._connection).get(
            None, entity=entity, **kw
        )


if __name__ == "__main__":
    payload = {}
    l = Locations(payload, None, None)
    l._validate_payload()

# ============= EOF =============================================
