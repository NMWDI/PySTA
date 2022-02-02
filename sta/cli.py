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
import shapefile
import click

from sta.client import Client


@click.group()
def cli():
    pass


@cli.command()
@click.option("--name")
@click.option("--agency")
@click.option("--verbose/--no-verbose", default=False)
@click.option("--out")
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

    woutput(out, records, query, client.base_url)


@cli.command()
@click.option("--name")
@click.option("--agency")
@click.option(
    "--pages",
    default=1,
    help="Number of pages of results to return. Each page is 1000 records by "
    "default",
)
@click.option("--verbose", default=False)
@click.option("--out")
def locations(name, agency, pages, verbose, out):
    client = Client()

    query = []
    if name:
        query.append(f"name eq '{name}'")

    if agency:
        query.append(f"properties/agency eq '{agency}'")

    query = " and ".join(query)
    if verbose:
        click.secho(f"query={query}")

    nrecords = woutput(
        out, client.get_locations(query=query, pages=pages), query, client.base_url
    )
    click.secho(f"wrote nrecords={nrecords} to {out}")


def woutput(out, *args, **kw):
    if out.endswith(".shp"):
        func = shp_output
    elif out.endswith(".csv"):
        func = csv_output
    else:
        func = json_output
    return func(out, *args, **kw)


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
