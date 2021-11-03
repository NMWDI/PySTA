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
import logging
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import pyproj
import requests
import re

from .definitions import OM_Measurement, FOOT

projections = {}

IDREGEX = re.compile(r"(?P<id>\(\d+\))")


def get_items(start_url):
    items = []

    def rget(url):
        resp = requests.get(url)
        data = resp.json()
        values = data["value"]
        logging.info("url={}, nvalues={}".format(url, len(values)))

        items.extend(values)
        try:
            next_url = data["@iot.nextLink"]
        except KeyError:
            return
        rget(next_url)

    rget(start_url)
    return items


def make_geometry_point_from_utm(e, n, zone=None, ellps=None, srid=None):
    if zone:
        if zone in projections:
            p = projections[zone]
        else:
            if ellps is None:
                ellps = "WGS84"
            p = pyproj.Proj(proj="utm", zone=int(zone), ellps=ellps)
            projections[zone] = p
    elif srid:
        # get zone
        if srid in projections:
            p = projections[srid]
            projections[srid] = p
        else:
            # p = pyproj.Proj(proj='utm', zone=int(zone), ellps='WGS84')
            p = pyproj.Proj("EPSG:{}".format(srid))

    lon, lat = p(e, n, inverse=True)
    return make_geometry_point_from_latlon(lat, lon)


def make_geometry_point_from_latlon(lat, lon):
    return {"type": "Point", "coordinates": [lon, lat]}


def iotid(iid):
    return {"@iot.id": iid}


class STAClient:
    def __init__(self, host, user, pwd, port):
        self._host = host
        self._user = user
        self._pwd = pwd
        self._port = port

    @staticmethod
    def make_st_time(ts):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                t = datetime.strptime(ts, fmt)
                return f"{t.isoformat()}.000Z"
            except BaseException:
                pass
        else:
            return ts

    def get_locations(self, fs=None, orderby=None):
        params = []
        base = "Locations"
        if fs:
            params.append(f"$filter={fs}")
        if orderby:
            params.append(f"$orderby={orderby}")

        if params:
            params = "&".join(params)
            base = f"{base}?{params}"

        url = self._make_url(base)

        return get_items(url)

    def delete_location(self, iotid):
        url = self._make_url(f"Locations({iotid})")
        self.delete(url)

    def put_observed_property(self, name, description, **kw):
        obsprop_id = self.get_observed_property(name)
        if obsprop_id is None:
            payload = {
                "name": name,
                "description": description,
                "definition": "No Definition",
            }
            obsprop_id = self._add("ObservedProperties", payload)

        return obsprop_id

    def put_sensor(self, name, description):
        sensor_id = self.get_sensor(name)
        if sensor_id is None:
            payload = {
                "name": name,
                "description": description,
                "encodingType": "application/pdf",
                "metadata": "No Metadata",
            }
            sensor_id = self._add("Sensors", payload)

        return sensor_id

    def put_datastream(
        self,
        name,
        description,
        thing_id,
        obsprop_id,
        sensor_id,
        properties=None,
        unit=None,
        otype=None,
    ):

        if unit is None:
            unit = FOOT
        if otype is None:
            otype = OM_Measurement

        payload = {
            "Thing": iotid(thing_id),
            "ObservedProperty": iotid(obsprop_id),
            "Sensor": iotid(sensor_id),
            "unitOfMeasurement": unit,
            "observationType": otype,
            "description": description,
            "name": name,
        }

        if properties:
            payload["properties"] = properties

        ds = self.get_datastream(name, thing_id)
        if ds:
            ds_id = ds["@iot.id"]
            if self._should_patch(ds, properties):
                self.patch_datastream(ds_id, payload)
            added = False
        else:
            ds_id = self._add("Datastreams", payload)
            added = True
        return ds_id, added

    def get_sensor(self, name):
        return self._get_id("Sensors", name)

    def get_observed_property(self, name):
        return self._get_id("ObservedProperties", name)

    def get_datastream(self, name, thing_id):
        tag = f"Things({thing_id})/Datastreams"
        vs = self._get_item_by_name(tag, name)
        if vs:
            return vs[0]

    def get_datastream_id(self, name, thing_id):
        tag = f"Things({thing_id})/Datastreams"
        return self._get_id(tag, name)

    def get_last_thing(self):
        pass

    def get_last_observation(self, datastream_id):
        url = self._make_url(
            f"Datastreams({datastream_id})/Observations?$orderby=phenomenonTime desc&$top=1"
        )
        logging.info(f"request url: {url}")
        resp = requests.get(url)
        v = resp.json()
        # logging.info(f'v {v}')

        vs = v.get("value")
        # logging.info(f'vs {vs}')
        if vs:
            return vs[0].get("phenomenonTime")

    def delete(self, url):
        resp = requests.delete(url, auth=(self._user, self._pwd))
        if resp.status_code != 200:
            logging.info(resp, resp.text)

    def patch(self, url, payload):
        resp = requests.patch(url, auth=(self._user, self._pwd), json=payload)
        if resp.status_code != 200:
            logging.info(resp, resp.text)

    def patch_thing(self, iotid, payload):
        url = self._make_url(f"Things({iotid})")
        self.patch(url, payload)

    def patch_location(self, iotid, payload):
        url = self._make_url(f"Locations({iotid})")
        self.patch(url, payload)

    def patch_datastream(self, iotid, payload):
        url = self._make_url(f"Datastreams({iotid})")
        self.patch(url, payload)

    def put_location(
        self, name, description, properties, utm=None, latlon=None, verbose=False
    ):
        lid = self.get_location_id(name)
        if lid is None:

            geometry = None
            if utm:
                geometry = make_geometry_point_from_utm(*utm)
            elif latlon:
                geometry = make_geometry_point_from_latlon(*latlon)

            if geometry:
                payload = {
                    "name": name,
                    "description": description,
                    "properties": properties,
                    "location": geometry,
                    "encodingType": "application/vnd.geo+json",
                }
                return self._add("Locations", payload, verbose=verbose), True
            else:
                logging.info(
                    "failed to construct geometry. need to specify utm or latlon"
                )
                raise Exception
        else:
            self.patch_location(
                lid, {"properties": properties, "description": description}
            )
            return lid, False

    def put_thing(
        self, name, description, properties, location_id, check=True, verbose=False
    ):
        tid = None
        if check:
            tid = self.get_thing_id(name, location_id)

        if tid is None:
            payload = {
                "name": name,
                "description": description,
                "properties": properties,
                "Locations": [{"@iot.id": location_id}],
            }
            return self._add("Things", payload, verbose=verbose)
        else:
            self.patch_thing(
                tid, {"properties": properties, "description": description}
            )
        return tid

    def add_observations(self, datastream_id, components, obs):
        if not obs:
            return

        n = 100
        nobs = len(obs)
        logging.info("nobservations: {}".format(nobs))
        for i in range(0, nobs, n):
            chunk = obs[i : i + n]
            pd = self.observation_payload(datastream_id, components, chunk)
            # logging.info('payload {}'.format(pd))
            url = self._make_url("CreateObservations")
            # logging.info('url: {}'.format(url))
            # logging.info('payload: {}'.format(pd))
            resp = requests.post(url, auth=("write", self._pwd), json=pd)
            logging.info("response {}, {}".format(i, resp))

    @staticmethod
    def observation_payload(datastream_id, components, data):
        obj = {
            "Datastream": {"@iot.id": datastream_id},
            "components": components,
            "dataArray": data,
        }
        return [obj]

    def get_location_id(self, name):
        return self._get_id("Locations", name)

    # def get_datastream_id(self, name, sensor_name, thing_id):
    #     tag = 'Datastreams'
    #     if thing_id:
    #         tag = f'Things({thing_id})/{tag}'
    #
    #     return self._get_id(tag, name, extra_args=f'$filter=Sensor/name eq \'{sensor_name}\'')

    def get_thing(self, **filters):
        base = self._make_base("Things", **filters)
        return self._get_item(base)

    def get_thing_id(self, name, location_id=None, location_name=None):
        tag = "Things"
        extra_args = None
        if location_id:
            tag = f"Locations({location_id})/{tag}"
        elif location_name:
            extra_args = f"$filter=Location/name eq '{location_name}'"

        return self._get_id(tag, name, extra_args=extra_args)

    @staticmethod
    def _should_patch(obj, properties):
        patch = True
        if all((v == obj["properties"].get(k) for k, v in properties.items() if v)):
            patch = False
        elif all((v == properties.get(k) for k, v in obj["properties"].items())):
            patch = False
        return patch

    @staticmethod
    def _make_base(tag, **filters):
        def factory(k, v):
            k = k.replace("__", "/")
            comp = "eq"
            return f"?filter={k} {comp} {v}"

        fs = [factory(k, v) for k, v in filters.items()]
        fs = "&".join(fs)
        return f"{tag}?{fs}"

    def _get_item(self, base, verbose=False):
        url = self._make_url(base)
        resp = requests.get(url, auth=("read", "read"))
        if verbose:
            logging.info(f"Get item {base}")

        j = resp.json()
        try:
            return j["value"]
        except KeyError:
            pass

    def _get_id(self, tag, name, verbose=False, **kw):
        vs = self._get_item_by_name(tag, name, **kw)
        if vs:
            iotid = vs[0]["@iot.id"]
            if verbose:
                logging.info(f"Got tag={tag} name={name} iotid={iotid}")
            return iotid

    def _get_item_by_name(self, tag, name, extra_args=None, verbose=False):
        tag = f"{tag}?$filter=name eq '{name}'"
        if extra_args:
            tag = f"{tag}&{extra_args}"
        return self._get_item(tag, verbose)

    def _add(self, tag, payload, extract_iotid=True, verbose=False):
        url = self._make_url(tag)
        if verbose:
            logging.info(f"Add url={url}")
            logging.info(f"Add payload={payload}")

        resp = requests.post(url, auth=(self._user, self._pwd), json=payload)

        if extract_iotid:
            m = IDREGEX.search(resp.headers.get("location", ""))
            # logging.info(f'Response={resp.json()}')

            if m:
                iotid = m.group("id")[1:-1]
                if verbose:
                    logging.info(f"added {tag} {iotid}")
                return iotid
            else:
                logging.info(f"failed adding {tag} {payload}")
                logging.info(f"Response={resp.json()}")

    def _make_url(self, tag):
        port = self._port
        if not port or port == 80:
            port = ""
        else:
            port = f":{port}"

        return f"http://{self._host}{port}/FROST-Server/v1.1/{tag}"


class STAMQTTClient:
    def __init__(self, host):
        self._client = mqtt.Client("STA")
        self._client.connect(host)

    def add_observations(self, datastream_id, payloads):
        client = self._client
        for payload in payloads:
            client.publish(
                f"v1.0/Datastreams({datastream_id})/Observations",
                payload=json.dumps(payload),
            )


# ============= EOF =============================================
