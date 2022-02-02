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

import requests
import shapefile
import click
import shapely.wkt
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon
from sta.client import Client


@click.group()
def cli():
    pass


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
@click.option("--verbose", default=False)
@click.option(
    "--out",
    default="out.json",
    help="Location to save file. use file extension to define output type. "
    "valid extensions are .shp, .csv, and .json. JSON output is used by "
    "default",
)
def locations(name, agency, query, pages, expand, within, bbox, verbose, out):
    client = Client()

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
                        print(f'invalid within argument "{within}"')
        if wkt:
            filterargs.append(f"st_within(location, geography'{wkt}')")

    query = " and ".join(filterargs)
    if verbose:
        click.secho(f"query={query}")

    if out == "out.json":
        out = "out.locations.json"

    woutput(
        out,
        client.get_locations(query=query, pages=pages, expand=expand),
        query,
        client.base_url,
    )


def statelookup(name):
    return {"NM": 35}[name]


def get_county_polygon(name):
    if ":" in name:
        state, county = name.split(":")
    else:
        state = "NM"
        county = name

    state = statelookup(state)

    url = f"https://reference.geoconnex.us/collections/counties/items?STATEFP={state}&f=json"
    resp = requests.get(url)

    obj = resp.json()
    for f in obj["features"]:
        if f["properties"]["NAME"] == county:
            return Polygon(f["geometry"]["coordinates"][0][0]).wkt


def woutput(out, *args, **kw):
    if out.endswith(".shp"):
        func = shp_output
    elif out.endswith(".csv"):
        func = csv_output
    else:
        func = json_output
    nrecords = func(out, *args, **kw)
    click.secho(f"wrote nrecords={nrecords} to {out}")
    return nrecords


def shp_output(out, records_generator, query, base_url):
    with shapefile.Writer(out) as w:
        w.field("TEXT", "C")
        nrecords = 0
        for row in records_generator:
            geom = row["location"]
            coords = geom["coordinates"]
            w.point(*coords)
            w.record(row["name"])
            nrecords += 1

    return nrecords


def json_output(out, records_generator, query, base_url):
    records = list(records_generator)
    data = {"data": records, "query": query, "base_url": base_url}
    with open(out, "w") as wfile:
        json.dump(data, wfile, indent=2)
    return len(records)


def csv_output(out, records_generator, query, base_url):
    with open(out, "w") as wfile:
        if out.endswith(".csv"):
            writer = csv.writer(wfile)
            count = 0
            for emp in records_generator:
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
