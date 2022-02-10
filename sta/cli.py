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
import csv
import json
import os
import pprint
from itertools import groupby

import requests
import shapefile
import click
import shapely.wkt
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon
from sta.client import Client


class ObsContainer:
    def __init__(self, location, thing, datastream, obs):
        self.location = location
        self.thing = thing
        self.datastream = datastream
        self.obs = obs

    def header(self):
        return (
            "location_name",
            "location_id",
            "thing_name",
            "thing_id",
            "datastream_name",
            "datastream_id",
            "phenomenonTime",
            "resultTime",
            "result",
        )

    def torow(self):
        return [
            [
                self.location["name"],
                self.location["@iot.id"],
                self.thing["name"],
                self.thing["@iot.id"],
                self.datastream["name"],
                self.datastream["@iot.id"],
                o["phenomenonTime"],
                o["resultTime"],
                o["result"],
            ]
            for o in self.obs
        ]

    def tojson(self):
        return {
            "location": self.location,
            "datastream": self.datastream,
            "thing": self.thing,
            "observations": self.obs,
        }


@click.group()
def cli():
    pass


@cli.group()
def water():
    pass


@water.command()
# query options
@click.option("--location")
@click.option("--agency")
@click.option("--within")
@click.option("--last", default=0)
# output options
@click.option("--out", default=None)
@click.option("--screen", is_flag=True)
@click.option("--verbose", is_flag=True)
def depths(location, agency, within, last, out, screen, verbose):
    water_obs(
        location, agency, within, last, out, screen, verbose, "Groundwater Levels"
    )


@water.command()
@click.option("--location")
@click.option("--agency")
@click.option("--within")
@click.option("--last", default=0)
@click.option("--out", default=None)
@click.option("--screen", is_flag=True)
@click.option("--verbose", is_flag=True)
def elevations(location, agency, within, last, out, screen, verbose):
    water_obs(
        location, agency, within, last, out, screen, verbose, "Groundwater Elevations"
    )


def water_obs(location, agency, within, last, out, screen, verbose, dsname):
    client = Client()
    filter_args = []
    if within:
        wkt = make_wkt(within)
        if wkt:
            filter_args.append(make_within(wkt))
    if agency:
        filter_args.append(f"properties/agency eq '{agency}'")

    name, query = None, None
    if location:
        if location.endswith("*"):
            filter_args.append(f"startswith(name, '{location[:-1]}')")
        else:
            filter_args.append(f"name eq '{name}'")
    if filter_args:
        query = " and ".join(filter_args)

    def obs_generator():
        for loc in client.get_locations(query=query):
            thing = client.get_thing(name="Water Well", location=loc)
            ds = client.get_datastream(name=dsname, thing=thing)

            orderby = None
            limit = None
            if last:
                limit = last
                orderby = "phenomenonTime desc"

            obss = list(
                client.get_observations(
                    ds, verbose=verbose, limit=limit, orderby=orderby
                )
            )

            count = len(obss)
            yield ObsContainer(loc, thing, ds, obss)
            # count = 0
            # for obs in client.get_observations(ds, verbose=verbose):
            #     count += 1
            #     yield obs

            click.secho(
                f"got observations {count} for location={loc['name']}, {loc['@iot.id']}\n",
                fg="green",
            )

    woutput(screen, out, obs_generator(), None, client.base_url)


@cli.command()
@click.option("--name")
@click.option("--agency")
@click.option("--verbose/--no-verbose", default=False)
@click.option("--out", default="out.json")
def things(name, agency, verbose, out):
    client = Client()

    query = []
    if name:
        query.append(f"name eq '{name}'")

    if agency:
        query.append(f"Locations/properties/agency eq '{agency}'")

    query = " and ".join(query)
    records = list(client.get_things(query if query else None))
    if verbose:
        for li in records:
            click.secho(li)

    cnt = len(records)
    click.secho(f"Found {cnt} Things")

    if out == "out.json":
        out = "out.things.json"

    woutput(out, records, query, client.base_url)


@cli.command()
@click.option("--name", help="Filter Locations by name")
@click.option("--agency", help="Filter Locations by agency")
@click.option("--query")
@click.option(
    "--pages",
    default=1,
    help="Number of pages of results to return. Each page is 1000 records by "
    "default. Results ordered by location.@iot.id ascending.  Use negative page numbers for "
    "descending sorting",
)
@click.option("--expand")
@click.option("--within")
@click.option("--bbox")
@click.option("--screen", is_flag=True)
@click.option("--verbose", is_flag=True)
@click.option(
    "--out",
    help="Location to save file. use file extension to define output type. "
    "valid extensions are .shp, .csv, and .json. JSON output is used by "
    "default",
)
@click.option("--url", default=None)
@click.option("--group", default=None)
@click.option("--names-only", is_flag=True)
def locations(
    name,
    agency,
    query,
    pages,
    expand,
    within,
    bbox,
    screen,
    verbose,
    out,
    url,
    group,
    names_only,
):
    client = Client(base_url=url)

    filterargs = []
    if name:
        filterargs.append(f"name eq '{name}'")

    if agency:
        filterargs.append(f"properties/agency eq '{agency}'")

    if query:
        filterargs.append(query)

    if bbox:
        # upper left, lower right
        pts = [[float(vi) for vi in pt.strip().split(" ")] for pt in bbox.split(",")]
        bbox = Polygon(
            [(pts[a][0], pts[b][1]) for a, b in [(0, 0), (1, 0), (1, 1), (0, 1)]]
        )
        filterargs.append(f"st_within(location, geography'{bbox}')")
    elif within:
        wkt = make_wkt(within)
        if wkt:
            filterargs.append(make_within(wkt))

    query = " and ".join(filterargs)
    # if verbose:
    #     click.secho(f"query={query}")

    if out == "out.json":
        out = "out.locations.json"

    woutput(
        screen,
        out,
        client.get_locations(query=query, pages=pages, expand=expand, verbose=verbose),
        query,
        client.base_url,
        group=group,
        names_only=names_only,
    )


def make_within(wkt):
    return f"st_within(location, geography'{wkt}')"


def make_wkt(within):
    wkt = None
    if os.path.isfile(within):
        # try to read in file
        if within.endswith(".geojson"):
            pass
        elif within.endswith(".shp"):
            pass
    else:
        # load a raw WKT object
        try:
            wkt = shapely.wkt.loads(within)
        except:
            # maybe its a name of a county
            wkt = get_county_polygon(within)
            if wkt is None:
                # not a WKT object probably a sequence of points that should
                # be interpreted as a polygon
                try:
                    wkt = Polygon(within.split(",")).wkt
                except:
                    warning(f'Invalid within argument "{within}"')
    return wkt


def statelookup(shortname):
    p = os.path.join(os.path.expanduser("~"), ".sta.states.json")
    if not os.path.isfile(p):
        click.secho(f"Caching states to {p}")
        url = f"https://reference.geoconnex.us/collections/states/items?f=json"
        resp = requests.get(url)
        with open(p, "w") as wfile:
            json.dump(resp.json(), wfile)

    with open(p, "r") as rfile:
        obj = json.load(rfile)

    shortname = shortname.lower()
    for f in obj["features"]:
        props = f["properties"]
        if props["STUSPS"].lower() == shortname:
            return props["STATEFP"]


def get_county_polygon(name):
    if ":" in name:
        state, county = name.split(":")
    else:
        state = "NM"
        county = name

    statefp = statelookup(state)
    if statefp:
        p = os.path.join(os.path.expanduser("~"), f".sta.{state}.counties.json")
        if not os.path.isfile(p):
            click.secho(f"Caching {state} counties to {p}")
            url = f"https://reference.geoconnex.us/collections/counties/items?STATEFP={statefp}&f=json"
            resp = requests.get(url)

            obj = resp.json()
            with open(p, "w") as wfile:
                json.dump(obj, wfile)

        with open(p, "r") as rfile:
            obj = json.load(rfile)

        county = county.lower()
        for f in obj["features"]:
            if f["properties"]["NAME"].lower() == county:
                return Polygon(f["geometry"]["coordinates"][0][0]).wkt
        else:
            warning(f"county '{county}' does not exist")
            warning("---------- Valid county names -------------")
            for f in obj["features"]:
                warning(f["properties"]["NAME"])
            warning("--------------------------------------------")
    else:
        warning(f"Invalid state. {state}")


def warning(msg):
    click.secho(msg, fg="red")


def woutput(screen, out, records_generator, *args, **kw):
    if not screen and not out:
        out = "out.json"

    print("screen", screen, out)
    if screen and out:
        records_generator = list(records_generator)

    names_only = kw.get("names_only", False)
    if screen or names_only:
        for i, r in enumerate(records_generator):
            if names_only:
                msg = r["name"]
            else:
                msg = f"{pprint.pformat(r)}\n"
            click.secho(f"{i + 1} -------------------", fg="yellow")
            click.secho(msg, fg="green")

    if out:
        if out.endswith(".shp"):
            func = shp_output
        elif out.endswith(".csv"):
            func = csv_output
        else:
            func = json_output

        nrecords = func(out, records_generator, *args, **kw)
        click.secho(f"wrote nrecords={nrecords} to {out}", fg="yellow")
        return nrecords


def shp_output(out, records_generator, query, base_url, group=False, **kw):
    nrecords = 0
    if group:

        def key(r):
            return r["properties"]["agency"]

        records = list(records_generator)
        for agency, records in groupby(sorted(records, key=key), key=key):
            flag = False
            outt, ext = os.path.splitext(out)
            outt = f"{outt}-{agency}{ext}"
            with shapefile.Writer(outt) as w:
                w.field("name", "C")
                for row in records:
                    properties = row["properties"]
                    if not flag:
                        for k, v in properties.items():
                            w.field(k, "C")
                        flag = True

                    geom = row["location"]
                    coords = geom["coordinates"]
                    w.point(*coords)
                    properties["name"] = row["name"]
                    w.record(**properties)
                    nrecords += 1

    else:
        with shapefile.Writer(out) as w:
            w.field("name", "C")
            w.field("agency", "C")

            for row in records_generator:
                properties = row["properties"]

                geom = row["location"]
                coords = geom["coordinates"]
                w.point(*coords)
                w.record(row["name"], properties["agency"])
                nrecords += 1

    return nrecords


def json_output(out, records_generator, query, base_url, **kw):
    records = list(records_generator)
    if isinstance(records[0], ObsContainer):
        records = [ri.tojson() for ri in records]

    data = {"data": records, "query": query, "base_url": base_url}
    with open(out, "w") as wfile:
        json.dump(data, wfile, indent=2)
    return len(records)


def csv_output(out, records_generator, query, base_url, **kw):
    with open(out, "w") as wfile:
        writer = csv.writer(wfile)
        count = 0

        for emp in records_generator:
            if isinstance(emp, ObsContainer):
                if count == 0:
                    writer.writerow(emp.header())

                rows = emp.torow()
                writer.writerows(rows)
                count += len(rows)
            else:
                if count == 0:
                    # Writing headers of CSV file
                    header = emp.keys()
                    writer.writerow(header)

                # Writing data of CSV file
                writer.writerow(emp.values())
            count += 1

        nrecords = count

    return nrecords


if __name__ == "__main__":
    locations()
# ============= EOF =============================================
